package skydbstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gitlab.com/NebulousLabs/fastrand"
	"log"
	"time"

	"github.com/skynetlabs/certmagic-storage-skydb/skydb"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"go.sia.tech/siad/crypto"
)

const (
	contentsAttribute    = "Contents"
	primaryKeyAttribute  = "PrimaryKey"
	lastUpdatedAttribute = "LastUpdated"
	lockTimeoutMinutes   = caddy.Duration(5 * time.Minute)
	lockPollingInterval  = caddy.Duration(5 * time.Second)
)

var (
	// The registry doesn't support DELETE as an operation. The next best thing
	// is to write an empty record to it. This is the empty record that we're
	// going to be writing.
	emptyRegistryEntry = [34]byte{}
)

// Item holds structure of domain, certificate data,
// and last updated for marshaling with SkyDB
type Item struct {
	PrimaryKey  string    `json:"PrimaryKey"`
	Contents    []byte    `json:"Contents"`
	LastUpdated time.Time `json:"LastUpdated"`
}

// ItemRecord is an intermediate record that holds a link to the Item value we
// want to store and retrieve from SkyDB but also a dataKey which points to a
// record that holds a full list of stored keys. This allows us to implement
// the List functionality.
type ItemRecord struct {
	Item            Item        `json:"Item"`
	KeysListDataKey crypto.Hash `json:"KeysListDataKey"`
}

// Storage implements certmagic.Storage to facilitate
// storage of certificates in DynamoDB for a clustered environment.
// Also implements certmagic.Locker to facilitate locking
// and unlocking of cert data during storage
type Storage struct {
	SkyDB               *skydb.SkyDB     `json:"-"`
	Table               string           `json:"table,omitempty"`
	AwsSession          *session.Session `json:"-"`
	SkyDBEndpoint       string           `json:"skydb_endpoint,omitempty"` // TODO Do I even need this? If so, use it as custom opts in initConfig.
	AwsRegion           string           `json:"aws_region,omitempty"`
	AwsDisableSSL       bool             `json:"aws_disable_ssl,omitempty"`
	LockTimeout         caddy.Duration   `json:"lock_timeout,omitempty"`
	LockPollingInterval caddy.Duration   `json:"lock_polling_interval,omitempty"`

	// revisionsCache stores the latest revision we have a for a given dataKey.
	// This allows us to avoid changing the interfaces used here by passing
	// revisions.
	revisionsCache map[string]uint64
}

// initConfig initializes configuration for table name and AWS session
func (s *Storage) initConfig() error {
	sdb, err := skydb.New()
	if err != nil {
		return err
	}
	s.SkyDB = sdb

	if s.LockTimeout == 0 {
		s.LockTimeout = lockTimeoutMinutes
	}
	if s.LockPollingInterval == 0 {
		s.LockPollingInterval = lockPollingInterval
	}
	return nil
}

// Store puts value at key.
func (s *Storage) Store(key string, value []byte) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	if key == "" {
		return errors.New("key must not be empty")
	}
	dataKey := crypto.HashBytes([]byte(key))
	// Get the item record.
	// Note: Getting the revision here and using it later has a theoretical
	// chance of creating a race. This is because of the registry's internal
	// behaviour of allowing an update of an existing revision if the hash of
	// the data that comes second is numerically higher than the hash of the
	// data that comes first. This isn't critical in our current use case.
	ir, rev, err := s.getItemRecord(key)
	if err != nil {
		return err
	}
	var keyList map[string]bool
	var keyListRev uint64 = -1 // we'll increment this before using it
	// Fetch the key list if it exists. Initialise it otherwise.
	if !isEmpty(ir.KeysListDataKey[:]) {
		// Get key list and its revision.
		var klData []byte
		klData, keyListRev, err = s.SkyDB.Read(ir.KeysListDataKey)
		if err != nil {
			return errors.New("failed to get key list from SkyDB. Error: " + err.Error())
		}
		err = json.Unmarshal(klData, &keyList)
		if err != nil {
			return errors.New("failed to unmarshal key list. Error: " + err.Error())
		}
	} else {
		// Create a dataKey for the list of keys.
		fastrand.Read(ir.KeysListDataKey[:])
	}

	keyListChanged := false
	// Are we deleting the entry? If so, remove the key from the key list.
	// Otherwise add it to the list.
	if slicesEqual(value, emptyRegistryEntry[:]) {
		delete(keyList, key)
		keyListChanged = true
	} else {
		_, exists := keyList[key]
		if !exists {
			keyList[key] = true
			keyListChanged = true
		}
	}
	if keyListChanged {
		bytes, err := json.Marshal(keyList)
		if err != nil {
			return errors.New("failed to serialise a new key list. Error: " + err.Error())
		}
		err = s.SkyDB.Write(bytes, ir.KeysListDataKey, keyListRev+1)
		if err != nil {
			return errors.New("failed to store the key list. Error: " + err.Error())
		}
	}

	// Update the item.
	ir.Item.PrimaryKey = key
	ir.Item.Contents = value
	ir.Item.LastUpdated = time.Now().UTC()
	// Store the key's new value
	bytes, err := json.Marshal(ir)
	if err != nil {
		return errors.New("failed to marshal the item record. Error: " + err.Error())
	}
	return s.SkyDB.Write(bytes, dataKey, rev)
}

// Load retrieves the value at key.
func (s *Storage) Load(key string) ([]byte, error) {
	if err := s.initConfig(); err != nil {
		return []byte{}, err
	}

	if key == "" {
		return []byte{}, errors.New("key must not be empty")
	}

	domainIR, _, err := s.getItemRecord(key)
	return []byte(domainIR.Item.Contents), err
}

// Delete deletes key.
func (s *Storage) Delete(key string) error {
	return s.Store(key, emptyRegistryEntry[:])
}

// Exists returns true if the key exists
// and there was no error checking.
func (s *Storage) Exists(key string) bool {

	cert, err := s.Load(key)
	if string(cert) != "" && err == nil {
		return true
	}

	return false
}

// List returns all keys that match prefix.
// If recursive is true, non-terminal keys
// will be enumerated (i.e. "directories"
// should be walked); otherwise, only keys
// prefixed exactly by prefix will be listed.
func (s *Storage) List(prefix string, recursive bool) ([]string, error) {
	if err := s.initConfig(); err != nil {
		return []string{}, err
	}

	if prefix == "" {
		return []string{}, errors.New("key prefix must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames: map[string]*string{
			"#D": aws.String(primaryKeyAttribute),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":p": {
				S: aws.String(prefix),
			},
		},
		FilterExpression: aws.String("begins_with(#D, :p)"),
		TableName:        aws.String(s.Table),
		ConsistentRead:   aws.Bool(true),
	}

	var matchingKeys []string
	pageNum := 0
	err := svc.ScanPages(input,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			pageNum++

			var items []Item
			err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &items)
			if err != nil {
				log.Printf("error unmarshaling page of items: %s", err.Error())
				return false
			}

			for _, i := range items {
				matchingKeys = append(matchingKeys, i.PrimaryKey)
			}

			return !lastPage
		})

	if err != nil {
		return []string{}, err
	}

	return matchingKeys, nil
}

// Stat returns information about key.
func (s *Storage) Stat(key string) (certmagic.KeyInfo, error) {
	domainIR, _, err := s.getItemRecord(key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        key,
		Modified:   domainIR.Item.LastUpdated,
		Size:       int64(len(domainIR.Item.Contents)),
		IsTerminal: true,
	}, nil
}

// Lock acquires the lock for key, blocking until the lock
// can be obtained or an error is returned. Note that, even
// after acquiring a lock, an idempotent operation may have
// already been performed by another process that acquired
// the lock before - so always check to make sure idempotent
// operations still need to be performed after acquiring the
// lock.
//
// The actual implementation of obtaining of a lock must be
// an atomic operation so that multiple Lock calls at the
// same time always results in only one caller receiving the
// lock at any given time.
//
// To prevent deadlocks, all implementations (where this concern
// is relevant) should put a reasonable expiration on the lock in
// case Unlock is unable to be called due to some sort of network
// failure or system crash.
func (s *Storage) Lock(ctx context.Context, key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("LOCK-%s", key)

	// Check for existing lock
	for {
		ir, _, err := s.getItemRecord(lockKey)
		if err != nil {
			return err
		}
		// if lock doesn't exist or is empty, break to create a new one
		if isEmpty(ir.Item.Contents) {
			break
		}
		// Lock exists, check if expired or sleep 5 seconds and check again
		expires, err := time.Parse(time.RFC3339, string(ir.Item.Contents))
		if err != nil {
			return err
		}
		if time.Now().After(expires) {
			if err := s.Unlock(key); err != nil {
				return err
			}
			break
		}

		select {
		case <-time.After(time.Duration(s.LockPollingInterval)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// lock doesn't exist, create it
	contents := []byte(time.Now().Add(time.Duration(s.LockTimeout)).Format(time.RFC3339))
	return s.Store(lockKey, contents)
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (s *Storage) Unlock(key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("LOCK-%s", key)

	return s.Delete(lockKey)
}

// getItemRecord fetches an ItemRecord from SkyDB.
func (s *Storage) getItemRecord(key string) (ItemRecord, uint64, error) {
	dataKey := crypto.HashBytes([]byte(key))
	data, rev, err := s.SkyDB.Read(dataKey)
	if err != nil {
		return ItemRecord{}, 0, err
	}
	// Check if `data` is empty, i.e. the item never existed.
	if isEmpty(data) {
		return ItemRecord{}, 0, nil
	}
	var ir ItemRecord
	err = json.Unmarshal(data, &ir)
	if err != nil {
		return ItemRecord{}, 0, err
	}
	return ir, rev, nil
}

func isEmpty(data []byte) bool {
	for _, v := range data {
		if v > 0 {
			return false
		}
	}
	return true
}

func slicesEqual(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}

// Interface guard
var _ certmagic.Storage = (*Storage)(nil)
