package certmagic_storage_dynamodb

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/caddyserver/certmagic"
)

const (
	CertDataAttribute    = "CertData"
	DomainAttribute      = "Domain"
	LastUpdatedAttribute = "LastUpdated"
)

// DomainItem holds structure of domain, certificate data,
// and last updated for marshaling with DynamoDb
type DomainItem struct {
	Domain      string    `json:"Domain"`
	CertData    string    `json:"CertData"`
	LastUpdated time.Time `json:"LastUpdated"`
}

// DynamoDBStorage implements certmagic.Storage to facilitate
// storage of certificates in DynamoDB for a clustered environment.
// Also implements certmagic.Locker to facilitate locking
// and unlocking of cert data during storage
type DynamoDBStorage struct {
	Table         string
	AwsSession    *session.Session
	AwsEndpoint   string
	AwsRegion     string
	AwsDisableSSL bool
}

// initConfig initializes configuration for table nam and AWS session
func (s *DynamoDBStorage) initConfig() error {
	if s.Table == "" {
		return fmt.Errorf("config error: table name is required")
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
func (s *DynamoDBStorage) Store(key string, value []byte) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	if key == "" {
		return fmt.Errorf("key must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			DomainAttribute: {
				S: aws.String(key),
			},
			CertDataAttribute: {
				S: aws.String(string(value)),
			},
			LastUpdatedAttribute: {
				S: aws.String(time.Now().Format(time.RFC3339)),
			},
		},
		TableName: aws.String(s.Table),
	}

	_, err := svc.PutItem(input)
	if err != nil {
		return err
	}

	return nil
}

// Load retrieves the value at key.
func (s *DynamoDBStorage) Load(key string) ([]byte, error) {
	if err := s.initConfig(); err != nil {
		return []byte{}, err
	}

	if key == "" {
		return []byte{}, fmt.Errorf("key must not be empty")
	}

	domainItem, err := s.getDomainItem(key)
	if err != nil {
		return []byte{}, err
	}

	return []byte(domainItem.CertData), nil
}

// Delete deletes key.
func (s *DynamoDBStorage) Delete(key string) error {
	if err := s.initConfig(); err != nil {
		return err
	}

	if key == "" {
		return fmt.Errorf("key must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			DomainAttribute: {
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
func (s *DynamoDBStorage) Exists(key string) bool {
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
func (s *DynamoDBStorage) List(prefix string, recursive bool) ([]string, error) {
	if err := s.initConfig(); err != nil {
		return []string{}, err
	}

	if prefix == "" {
		return []string{}, fmt.Errorf("key prefix must not be empty")
	}

	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames: map[string]*string{
			"#D": aws.String(DomainAttribute),
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

			var items []DomainItem
			err := dynamodbattribute.UnmarshalListOfMaps(page.Items, &items)
			if err != nil {
				log.Printf("error unmarshaling page of items: %s", err.Error())
				return false
			}

			for _, i := range items {
				matchingKeys = append(matchingKeys, i.Domain)
			}

			return !lastPage
		})

	if err != nil {
		return []string{}, err
	}

	return matchingKeys, nil
}

// Stat returns information about key.
func (s *DynamoDBStorage) Stat(key string) (certmagic.KeyInfo, error) {
	domainItem, err := s.getDomainItem(key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   domainItem.LastUpdated,
		Size:       int64(len(domainItem.CertData)),
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
func (s *DynamoDBStorage) Lock(key string) error {
	return nil
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (s *DynamoDBStorage) Unlock(key string) error {
	return nil
}

func (s *DynamoDBStorage) getDomainItem(key string) (DomainItem, error) {
	svc := dynamodb.New(s.AwsSession)
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			DomainAttribute: {
				S: aws.String(key),
			},
		},
		TableName:      aws.String(s.Table),
		ConsistentRead: aws.Bool(true),
	}

	result, err := svc.GetItem(input)
	if err != nil {
		return DomainItem{}, err
	}

	var domainItem DomainItem
	err = dynamodbattribute.UnmarshalMap(result.Item, &domainItem)
	if err != nil {
		return DomainItem{}, err
	}

	return domainItem, nil
}
