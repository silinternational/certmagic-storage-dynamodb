package dynamodbstorage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

const (
	contentsAttribute    = "Contents"
	primaryKeyAttribute  = "PrimaryKey"
	lastUpdatedAttribute = "LastUpdated"
	lockIdAttribute      = "LockID"
	expiresAtAttribute   = "ExpiresAt"
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
	// Table - [required] DynamoDB table name
	Table  string           `json:"table,omitempty"`
	Client *dynamodb.Client `json:"-"`

	// AwsEndpoint - [optional] provide an override for DynamoDB service.
	// By default it'll use the standard production DynamoDB endpoints.
	// Useful for testing with a local DynamoDB instance.
	AwsEndpoint string `json:"aws_endpoint,omitempty"`

	// AwsRegion - [optional] region using DynamoDB in.
	// Useful for testing with a local DynamoDB instance.
	AwsRegion string `json:"aws_region,omitempty"`

	// AwsDisableSSL - [optional] disable SSL for DynamoDB connections. Default: false
	// Only useful for local testing, do not use outside of local testing.
	AwsDisableSSL bool `json:"aws_disable_ssl,omitempty"`

	// LockTimeout - [optional] how long to wait for a lock to be created. Default: 5 minutes
	LockTimeout caddy.Duration `json:"lock_timeout,omitempty"`

	// LockPollingInterval - [optional] how often to check for lock released. Default: 5 seconds
	LockPollingInterval caddy.Duration `json:"lock_polling_interval,omitempty"`

	// LockRefreshInterval - [optional] how often to refresh the lock. Default: LockTimeout / 3
	LockRefreshInterval caddy.Duration `json:"lock_refresh_interval,omitempty"`

	locks *sync.Map // map[string]*LockHandle
}

// LockHandle holds the information of a lock
type LockHandle struct {
	Key        string
	LockID     string             // UUID to identify the lock
	cancelFunc context.CancelFunc // Function to cancel periodic refresh
}

// initConfig initializes configuration for table name and AWS client
func (s *Storage) initConfig(ctx context.Context) error {
	if s.Table == "" {
		return errors.New("config error: table name is required")
	}

	if s.LockTimeout == 0 {
		s.LockTimeout = lockTimeoutMinutes
	}
	if s.LockPollingInterval == 0 {
		s.LockPollingInterval = lockPollingInterval
	}
	if s.LockRefreshInterval == 0 {
		s.LockRefreshInterval = s.LockTimeout / 3
	}

	// Initialize AWS Client if needed
	if s.Client == nil {
		cfg, err := config.LoadDefaultConfig(
			ctx,
			config.WithRegion(s.AwsRegion),
			config.WithBaseEndpoint(s.AwsEndpoint),
		)
		if err != nil {
			return err
		}

		s.Client = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.EndpointOptions.DisableHTTPS = s.AwsDisableSSL
		})
	}

	if s.locks == nil {
		s.locks = &sync.Map{}
	}

	return nil
}

// Store puts value at key.
func (s *Storage) Store(ctx context.Context, key string, value []byte) error {
	if err := s.initConfig(ctx); err != nil {
		return err
	}

	encVal := base64.StdEncoding.EncodeToString(value)

	if key == "" {
		return errors.New("key must not be empty")
	}

	input := &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			primaryKeyAttribute: &types.AttributeValueMemberS{
				Value: key,
			},
			contentsAttribute: &types.AttributeValueMemberS{
				Value: encVal,
			},
			lastUpdatedAttribute: &types.AttributeValueMemberS{
				Value: time.Now().Format(time.RFC3339),
			},
		},
		TableName: aws.String(s.Table),
	}

	_, err := s.Client.PutItem(ctx, input)
	return err
}

// Load retrieves the value at key.
func (s *Storage) Load(ctx context.Context, key string) ([]byte, error) {
	if err := s.initConfig(ctx); err != nil {
		return []byte{}, err
	}

	if key == "" {
		return []byte{}, errors.New("key must not be empty")
	}

	domainItem, err := s.getItem(ctx, key)
	return []byte(domainItem.Contents), err
}

// Delete deletes key.
func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := s.initConfig(ctx); err != nil {
		return err
	}

	if key == "" {
		return errors.New("key must not be empty")
	}

	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			primaryKeyAttribute: &types.AttributeValueMemberS{
				Value: key,
			},
		},
		TableName: aws.String(s.Table),
	}

	_, err := s.Client.DeleteItem(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// Exists returns true if the key exists
// and there was no error checking.
func (s *Storage) Exists(ctx context.Context, key string) bool {
	cert, err := s.Load(ctx, key)
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
func (s *Storage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if err := s.initConfig(ctx); err != nil {
		return []string{}, err
	}

	if prefix == "" {
		return []string{}, errors.New("key prefix must not be empty")
	}

	input := &dynamodb.ScanInput{
		ExpressionAttributeNames: map[string]string{
			"#D": primaryKeyAttribute,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{
				Value: prefix,
			},
		},
		FilterExpression: aws.String("begins_with(#D, :p)"),
		TableName:        aws.String(s.Table),
		ConsistentRead:   aws.Bool(true),
	}

	var matchingKeys []string

	paginator := dynamodb.NewScanPaginator(s.Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("failed to retrieve page, %v", err)
		}

		var pageItems []Item
		err = attributevalue.UnmarshalListOfMaps(page.Items, &pageItems)
		if err != nil {
			log.Printf("error unmarshalling page of items: %s", err.Error())
			return nil, err
		}

		for i := range pageItems {
			matchingKeys = append(matchingKeys, pageItems[i].PrimaryKey)
		}
	}

	return matchingKeys, nil
}

// Stat returns information about key.
func (s *Storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	domainItem, err := s.getItem(ctx, key)
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
func (s *Storage) Lock(ctx context.Context, key string) error {
	if err := s.initConfig(ctx); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("LOCK-%s", key)
	lockID := uuid.NewString()
	expiresAt := time.Now().Add(time.Duration(s.LockTimeout)).Unix()

	for {
		// Acquire lock if it doesn't exist or expired
		input := &dynamodb.PutItemInput{
			TableName: aws.String(s.Table),
			Item: map[string]types.AttributeValue{
				primaryKeyAttribute: &types.AttributeValueMemberS{Value: lockKey},
				lockIdAttribute:     &types.AttributeValueMemberS{Value: lockID},
				expiresAtAttribute:  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", expiresAt)},
			},
			ConditionExpression: aws.String(fmt.Sprintf("attribute_not_exists(%s) OR %s < :now", primaryKeyAttribute, expiresAtAttribute)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":now": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())},
			},
		}

		_, err := s.Client.PutItem(ctx, input)

		// Lock acquired successfully
		if err == nil {
			lockCtx, cancel := context.WithCancel(context.Background())
			lockHandle := &LockHandle{
				Key:        key,
				LockID:     lockID,
				cancelFunc: cancel,
			}
			s.locks.Store(key, lockHandle)

			// Start periodic refresh
			go s.keepLockFresh(lockCtx, lockHandle)

			return nil
		}

		// Lock not acquired, retry
		select {
		case <-time.After(time.Duration(s.LockPollingInterval)):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// keepLockFresh periodically updates the lock expiration
// to prevent it from expiring while the critical section
// is still running.
func (s *Storage) keepLockFresh(ctx context.Context, handle *LockHandle) {
	ticker := time.NewTicker(time.Duration(s.LockRefreshInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.updateLockExpiration(ctx, handle); err != nil {
				// The critical section should be aborted if lock refresh fails.
				// However, there is no way to notify the critical section to abort,
				// so we just log the error and stop refreshing the lock.
				log.Printf("failed to update lock expiration for key %s: %v", handle.Key, err)
				return
			}
		case <-ctx.Done():
			return // Unlock or external cancellation
		}
	}
}

// updateLockExpiration updates the lock expiration atomically.
func (s *Storage) updateLockExpiration(ctx context.Context, handle *LockHandle) error {
	lockKey := fmt.Sprintf("LOCK-%s", handle.Key)
	newExpiresAt := time.Now().Add(time.Duration(s.LockTimeout)).Unix()

	// Check LockID in ConditionExpression and update only the lock created by itself
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			primaryKeyAttribute: &types.AttributeValueMemberS{Value: lockKey},
		},
		UpdateExpression:    aws.String(fmt.Sprintf("SET %s = :newExpiresAt", expiresAtAttribute)),
		ConditionExpression: aws.String(fmt.Sprintf("%s = :lockID", lockIdAttribute)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newExpiresAt": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newExpiresAt)},
			":lockID":       &types.AttributeValueMemberS{Value: handle.LockID},
		},
	}

	var err error
	for range 3 { // Simple retry logic (up to 3 times)
		_, err = s.Client.UpdateItem(ctx, input)
		if err == nil {
			return nil // Update successful
		}

		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			// Do not retry if lock deleted or acquired by another process
			return fmt.Errorf("failed to update lock expiration: lock may have been deleted or updated by another process")
		}

		// Retry in case of network error or other transient issues
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("failed to update lock expiration after retries: %w", err)
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (s *Storage) Unlock(ctx context.Context, key string) error {
	if err := s.initConfig(ctx); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("LOCK-%s", key)

	handle, ok := s.locks.LoadAndDelete(key)
	if !ok {
		return fmt.Errorf("no lock handle found for key: %s", key)
	}
	lockHandle, _ := handle.(*LockHandle)

	// Stop periodic refresh of lock expiration
	lockHandle.cancelFunc()

	// Delete lock only if it was created by itself
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(s.Table),
		Key: map[string]types.AttributeValue{
			primaryKeyAttribute: &types.AttributeValueMemberS{Value: lockKey},
		},
		ConditionExpression: aws.String(fmt.Sprintf("%s = :lockID", lockIdAttribute)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lockID": &types.AttributeValueMemberS{Value: lockHandle.LockID},
		},
	}

	_, err := s.Client.DeleteItem(ctx, input)

	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			// Lock already deleted or updated by another process
			return fmt.Errorf("failed to unlock: lock may have been deleted or updated by another process")
		}
		return err
	}

	return nil
}

func (s *Storage) getItem(ctx context.Context, key string) (Item, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			primaryKeyAttribute: &types.AttributeValueMemberS{
				Value: key,
			},
		},
		TableName:      aws.String(s.Table),
		ConsistentRead: aws.Bool(true),
	}

	result, err := s.Client.GetItem(ctx, input)
	if err != nil {
		return Item{}, err
	}

	var domainItem Item
	err = attributevalue.UnmarshalMap(result.Item, &domainItem)
	if err != nil {
		return Item{}, err
	}
	if domainItem.Contents == "" {
		return Item{}, fs.ErrNotExist
	}

	dec, err := base64.StdEncoding.DecodeString(domainItem.Contents)
	if err != nil {
		return Item{}, err
	}
	domainItem.Contents = string(dec)

	return domainItem, nil
}

// Interface guard
var _ certmagic.Storage = (*Storage)(nil)
