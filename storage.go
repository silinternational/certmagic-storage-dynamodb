package dynamodbstorage

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	caddy "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

const (
	contentsAttribute    = "Contents"
	primaryKeyAttribute  = "PrimaryKey"
	lastUpdatedAttribute = "LastUpdated"
	lockTimeoutMinutes   = caddy.Duration(5 * time.Minute)
	lockPollingInterval  = caddy.Duration(5 * time.Second)
)

// Item holds structure of domain, certificate data,
// and last updated for marshaling with DynamoDb
type Item struct {
	PrimaryKey  string    `json:"PrimaryKey"`
	Contents    string    `json:"Contents"`
	LastUpdated time.Time `json:"LastUpdated"`
}

// Storage implements certmagic.Storage to facilitate
// storage of certificates in DynamoDB for a clustered environment.
// Also implements certmagic.Locker to facilitate locking
// and unlocking of cert data during storage
type Storage struct {
	Table               string           `json:"table,omitempty"`
	AwsSession          *session.Session `json:"-"`
	AwsEndpoint         string           `json:"aws_endpoint,omitempty"`
	AwsRegion           string           `json:"aws_region,omitempty"`
	AwsDisableSSL       bool             `json:"aws_disable_ssl,omitempty"`
	LockTimeout         caddy.Duration   `json:"lock_timeout,omitempty"`
	LockPollingInterval caddy.Duration   `json:"lock_polling_interval,omitempty"`
}

// initConfig initializes configuration for table name and AWS session
func (s *Storage) initConfig() error {
	if s.Table == "" {
		return errors.New("config error: table name is required")
	}

	if s.LockTimeout == 0 {
		s.LockTimeout = lockTimeoutMinutes
	}
	if s.LockPollingInterval == 0 {
		s.LockPollingInterval = lockPollingInterval
	}

	// Initialize AWS Session if needed
	if s.AwsSession == nil {
		var err error
		s.AwsSession, err = session.NewSession(&aws.Config{
			Endpoint:   &s.AwsEndpoint,
			Region:     &s.AwsRegion,
			DisableSSL: &s.AwsDisableSSL,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Store puts value at key.
func (s *Storage) Store(key string, value []byte) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	encVal := base64.StdEncoding.EncodeToString(value)

	if key == "" {
		return errors.New("key must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			primaryKeyAttribute: {
				S: aws.String(key),
			},
			contentsAttribute: {
				S: aws.String(encVal),
			},
			lastUpdatedAttribute: {
				S: aws.String(time.Now().Format(time.RFC3339)),
			},
		},
		TableName: aws.String(s.Table),
	}

	_, err := svc.PutItem(input)
	return err
}

// Load retrieves the value at key.
func (s *Storage) Load(key string) ([]byte, error) {
	if err := s.initConfig(); err != nil {
		return []byte{}, err
	}

	if key == "" {
		return []byte{}, errors.New("key must not be empty")
	}

	domainItem, err := s.getItem(key)
	return []byte(domainItem.Contents), err
}

// Delete deletes key.
func (s *Storage) Delete(key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	if key == "" {
		return errors.New("key must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			primaryKeyAttribute: {
				S: aws.String(key),
			},
		},
		TableName: aws.String(s.Table),
	}

	_, err := svc.DeleteItem(input)
	if err != nil {
		return err
	}

	return nil
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

	domainItem, err := s.getItem(key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   domainItem.LastUpdated,
		Size:       int64(len(domainItem.Contents)),
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
func (s *Storage) Lock(key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("LOCK-%s", key)

	// Check for existing lock
	for {
		existing, err := s.getItem(lockKey)
		_, isErrNotExists := err.(certmagic.ErrNotExist)
		if err != nil && !isErrNotExists {
			return err
		}

		// if lock doesn't exist or is empty, break to create a new one
		if isErrNotExists || existing.Contents == "" {
			break
		}

		// Lock exists, check if expired or sleep 5 seconds and check again
		expires, err := time.Parse(time.RFC3339, existing.Contents)
		if err != nil {
			return err
		}
		if time.Now().After(expires) {
			if err := s.Unlock(key); err != nil {
				return err
			}
			break
		}

		time.Sleep(time.Duration(s.LockPollingInterval))
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

func (s *Storage) getItem(key string) (Item, error) {
	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			primaryKeyAttribute: {
				S: aws.String(key),
			},
		},
		TableName:      aws.String(s.Table),
		ConsistentRead: aws.Bool(true),
	}

	result, err := svc.GetItem(input)
	if err != nil {
		return Item{}, err
	}

	var domainItem Item
	err = dynamodbattribute.UnmarshalMap(result.Item, &domainItem)
	if err != nil {
		return Item{}, err
	}
	if domainItem.Contents == "" {
		return Item{}, certmagic.ErrNotExist(fmt.Errorf("key %s doesn't exist", key))
	}

	dec, err := base64.StdEncoding.DecodeString(domainItem.Contents)
	if err != nil {
		return Item{}, err
	}
	domainItem.Contents = string(dec)

	return domainItem, nil
}
