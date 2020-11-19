package dynamodbstorage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"

	caddy "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

const (
	contentsAttribute            = "Contents"
	primaryKeyAttribute          = "PrimaryKey"
	lastUpdatedAttribute         = "LastUpdated"
	lockAttribute                = "Lock"
	defaultLockTimeoutMinutes    = caddy.Duration(1 * time.Minute)
	defaultLockFreshnessInterval = caddy.Duration(10 * time.Second)
	defaultLockPollingInterval   = caddy.Duration(1 * time.Second)
	stackTraceBufferSize         = 1024 * 128
	eFreshLockFileMessage        = "fresh lock file exists: "
	eDynamoNotFound              = "dynamo: no item found"
)

// Item holds structure of domain, certificate data,
// and last updated for marshaling with DynamoDb
type Item struct {
	PrimaryKey  string
	Lock        int64
	Contents    []byte `dynamo:",omitempty"`
	LastUpdated time.Time
}

// Storage implements certmagic.Storage to facilitate
// storage of certificates in DynamoDB for a clustered environment.
// Also implements certmagic.Locker to facilitate locking
// and unlocking of cert data during storage
type Storage struct {
	TableName             string         `json:"table,omitempty"`
	Dynamo                *dynamo.DB     `json:"-"`
	Table                 dynamo.Table   `json:"-"`
	AwsEndpoint           string         `json:"aws_endpoint,omitempty"`
	AwsRegion             string         `json:"aws_region,omitempty"`
	AwsDisableSSL         bool           `json:"aws_disable_ssl,omitempty"`
	LockTimeout           caddy.Duration `json:"lock_timeout,omitempty"`
	LockFreshnessInterval caddy.Duration `json:"lock_freshness_interval,omitempty"`
	LockPollingInterval   caddy.Duration `json:"lock_polling_interval,omitempty"`
}

// initConfig initializes configuration for table name and AWS session
func (s *Storage) initConfig() error {
	if s.TableName == "" {
		return errors.New("config error: table name is required")
	}
	if s.LockTimeout == 0 {
		s.LockTimeout = defaultLockTimeoutMinutes
	}
	if s.LockFreshnessInterval == 0 {
		s.LockFreshnessInterval = defaultLockFreshnessInterval
	}
	if s.LockPollingInterval == 0 {
		s.LockPollingInterval = defaultLockPollingInterval
	}
	// Initialize DDB client, if needed
	if s.Dynamo == nil {
		s.Dynamo = dynamo.New(session.New(), &aws.Config{
			Endpoint:   &s.AwsEndpoint,
			Region:     &s.AwsRegion,
			DisableSSL: &s.AwsDisableSSL,
		})
		s.Table = s.Dynamo.Table(s.TableName)
	}
	return nil
}

// Store puts value at key.
func (s *Storage) Store(key string, value []byte) error {
	if err := s.initConfig(); err != nil {
		return err
	}
	return s.Table.
		Update(primaryKeyAttribute, key).
		Set(contentsAttribute, value).
		Set(lastUpdatedAttribute, time.Now()).
		Run()
}

// Load retrieves the value at key.
func (s *Storage) Load(key string) ([]byte, error) {
	if err := s.initConfig(); err != nil {
		return []byte{}, err
	}
	if key == "" {
		return []byte{}, errors.New("key must not be empty")
	}
	domainItem, err := s.readItem(key)
	return domainItem.Contents, err
}

// Delete deletes key.
func (s *Storage) Delete(key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}
	if key == "" {
		return errors.New("key must not be empty")
	}
	return s.Table.Delete(primaryKeyAttribute, key).Run()
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

func (s *Storage) buildListScan(prefix string) *dynamo.Scan {
	return s.Table.Scan().
		Project(primaryKeyAttribute). // this is the only attribute we need for list
		Filter("begins_with($, ?)", primaryKeyAttribute, prefix).
		Consistent(true)
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
	var offset dynamo.PagingKey
	var results []string
	for {
		itr := s.Table.Scan().
			Project(primaryKeyAttribute). // this is the only attribute we need for list
			Filter("begins_with($, ?)", primaryKeyAttribute, prefix).
			Consistent(true).
			StartFrom(offset).
			Iter()
		if itr.Err() != nil {
			return []string{}, itr.Err()
		}
		for {
			var item Item
			more := itr.Next(&item)
			if !more {
				break
			}
			results = append(results, item.PrimaryKey)
		}
		// no more pages to load
		if itr.LastEvaluatedKey() == nil {
			break
		}
	}
	return results, nil
}

// Stat returns information about key.
func (s *Storage) Stat(key string) (certmagic.KeyInfo, error) {
	item, err := s.readItem(key)
	if err != nil {
		if err.Error() == eDynamoNotFound {
			return certmagic.KeyInfo{}, certmagic.ErrNotExist(fmt.Errorf("key %s doesn't exist", key))
		}
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        key,
		Modified:   item.LastUpdated,
		Size:       int64(len(item.Contents)),
		IsTerminal: true,
	}, nil
}

func (s *Storage) lockIsExpired(expiresTimestamp int64) bool {
	return time.Now().Unix() > expiresTimestamp
}

func (s *Storage) createLockNew(key string) error {
	item := Item{
		PrimaryKey:  key,
		Lock:        time.Now().Add(time.Duration(s.LockTimeout)).Unix(),
		LastUpdated: time.Now(),
	}
	// a record should not exist for key at this moment so if it does
	// it means another process created it and we should fail
	err := s.Table.Put(item).
		If("attribute_not_exists($)", primaryKeyAttribute).
		Run()
	if err != nil {
		return err
	}
	go s.keepLockFresh(key)
	return nil
}

func (s *Storage) createLockExisting(key string, lastUpdated time.Time) error {
	// no or stale lock -> idempotent create
	err := s.touchLock(key, lastUpdated)
	// if error here it likely means our conditional request failed and another process
	// acquired a lock in between our read and write
	if err != nil {
		return err
	}
	go s.keepLockFresh(key)
	return nil
}

func (s *Storage) createLock(key string) error {
	existing, err := s.readItem(key)
	if err == nil {
		// no error and lock is fresh
		if !s.lockIsExpired(existing.Lock) {
			return errors.New(eFreshLockFileMessage + key)
		}
		return s.createLockExisting(key, existing.LastUpdated)
	}
	// not found is ok. it means we need to create a new record
	if err.Error() == eDynamoNotFound {
		return s.createLockNew(key)
	}
	return err
}

func (s *Storage) removeLockfile(key string) error {
	existing, err := s.readItem(key)
	if err != nil {
		// record not found so no lock exists
		if err.Error() == eDynamoNotFound {
			return nil
		}
		return err
	}
	// unlocked by another process
	if existing.Lock == 0 {
		return nil
	}
	return s.createItemUpdate(key).
		Set(lockAttribute, 0).                                   // unlike with touch, we set to zero connote lock release
		If("$ = ?", lastUpdatedAttribute, existing.LastUpdated). // make sure record unchanged
		Run()
}

// keepLockfileFresh continuously updates the lock file
// at filename with the current timestamp. It stops
// when the file disappears (happy path = lock released),
// or when there is an error at any point. Since it polls
// every lockFreshnessInterval, this function might
// not terminate until up to lockFreshnessInterval after
// the lock is released.
func (s *Storage) keepLockFresh(key string) {
	defer func() {
		if err := recover(); err != nil {
			// errors here
			buf := make([]byte, stackTraceBufferSize)
			buf = buf[:runtime.Stack(buf, false)]
			log.Printf("panic: active locking: %v\n%s", err, buf)
		}
	}()

	for {
		time.Sleep(time.Duration(s.LockFreshnessInterval))
		done, err := s.updateLockFreshness(key)
		if err != nil {
			log.Printf("[ERROR] Keeping dynamodb lock fresh: %v - terminating lock maintenance (lock target: %s)", err, key)
			return
		}
		if done {
			return
		}
	}
}

// updateLockfileFreshness updates the lock file at filename
// with the current timestamp. It returns true if the parent
// loop can terminate (i.e. no more need to update the lock).
func (s *Storage) updateLockFreshness(key string) (bool, error) {
	existing, err := s.readItem(key)
	// if there was an error or lock was released we're done here
	if err != nil || existing.Lock == 0 {
		return true, err
	}
	// otherwise we touch lock and move on
	err = s.touchLock(key, existing.LastUpdated)
	return false, err
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
	for {
		err := s.createLock(key)
		if err == nil {
			// got the lock, yay
			return nil
		}
		// FIXME: there has to be a better way to check for errors
		if strings.HasPrefix(err.Error(), eFreshLockFileMessage) {
			// lockfile exists and is not stale;
			// just wait a moment and try again,
			// or return if context cancelled
			select {
			case <-time.After(time.Duration(s.LockPollingInterval)):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// unexpected error
		return fmt.Errorf("creating lock: %v", err)
	}
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (s *Storage) Unlock(key string) error {
	return s.removeLockfile(key)
}

func (s *Storage) readItem(key string) (Item, error) {
	var result Item
	err := s.Table.
		Get(primaryKeyAttribute, key).
		Consistent(true).
		One(&result)
	if err != nil {
		return Item{}, err
	}
	return result, nil
}

func (s *Storage) createItemUpdate(key string) *dynamo.Update {
	return s.Table.Update(primaryKeyAttribute, key).
		Set(lastUpdatedAttribute, time.Now())
}

func (s *Storage) touchLock(key string, lastUpdated time.Time) error {
	return s.createItemUpdate(key).
		Set(lockAttribute, time.Now().Add(time.Duration(s.LockTimeout)).Unix()).
		If("$ = ?", lastUpdatedAttribute, lastUpdated). // make sure record unchanged
		Run()
}

// Interface guard
var _ certmagic.Storage = (*Storage)(nil)
