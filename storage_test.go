package dynamodbstorage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/caddyserver/caddy/v2"
)

const (
	TestTableName = "CertMagicTest"
	DisableSSL    = true
)

func initDb() error {
	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: true,
	}

	ctx := context.Background()
	if err := storage.initConfig(ctx); err != nil {
		return err
	}

	// attempt to delete table in case already exists
	deleteTable := &dynamodb.DeleteTableInput{
		TableName: aws.String(storage.Table),
	}
	_, _ = storage.Client.DeleteTable(ctx, deleteTable)

	// create table
	createTable := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PrimaryKey"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PrimaryKey"),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},
		TableName: aws.String(storage.Table),
	}
	_, err := storage.Client.CreateTable(ctx, createTable)
	return err
}

func TestDynamoDBStorage_initConfg(t *testing.T) {
	type fields struct {
		Table         string
		KeyPrefix     string
		ColumnName    string
		AwsEndpoint   string
		AwsRegion     string
		AwsDisableSSL bool
	}
	tests := []struct {
		name     string
		fields   fields
		wantErr  bool
		expected *Storage
	}{
		{
			name:     "defaults - should error with empty table",
			fields:   fields{},
			wantErr:  true,
			expected: &Storage{},
		},
		{
			name: "defaults - provide only table name",
			fields: fields{
				Table: "Testing123",
			},
			wantErr: false,
			expected: &Storage{
				Table:               "Testing123",
				LockTimeout:         lockTimeoutMinutes,
				LockPollingInterval: lockPollingInterval,
				LockRefreshInterval: lockTimeoutMinutes / 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{
				Table:         tt.fields.Table,
				AwsEndpoint:   tt.fields.AwsEndpoint,
				AwsRegion:     tt.fields.AwsRegion,
				AwsDisableSSL: tt.fields.AwsDisableSSL,
			}
			if err := s.initConfig(context.Background()); (err != nil) != tt.wantErr {
				t.Errorf("initConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// unset client since it is too complicated for reflection testing
			s.Client = nil
			// unset locks since it is a pointer and will always be different
			s.locks = nil
			if !reflect.DeepEqual(tt.expected, s) {
				t.Errorf("Expected does not match actual: %+v != %+v.", tt.expected, s)
			}
		})
	}
}

func TestDynamoDBStorage_Store(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	type fields struct {
		Table         string
		KeyPrefix     string
		ColumnName    string
		AwsEndpoint   string
		AwsRegion     string
		AwsDisableSSL bool
	}
	type args struct {
		key   string
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "simple key/value store",
			fields: fields{
				Table:         TestTableName,
				AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
				AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
				AwsDisableSSL: DisableSSL,
			},
			args: args{
				key:   "simple-key",
				value: []byte("value"),
			},
			wantErr: false,
		},
		{
			name: "empty key should error",
			fields: fields{
				Table:         TestTableName,
				AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
				AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
				AwsDisableSSL: DisableSSL,
			},
			args: args{
				key:   "",
				value: []byte("value"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{
				Table:         tt.fields.Table,
				AwsEndpoint:   tt.fields.AwsEndpoint,
				AwsRegion:     tt.fields.AwsRegion,
				AwsDisableSSL: tt.fields.AwsDisableSSL,
			}
			err := s.Store(context.Background(), tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Store() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				loaded, err := s.Load(context.Background(), tt.args.key)
				if err != nil {
					t.Errorf("failed to load after store: %s", err.Error())
					return
				}
				if string(loaded) != string(tt.args.value) {
					t.Errorf("Load() returned value other than expected. Expected: %s, Actual: %s", string(tt.args.value), string(loaded))
					return
				}
			}
		})
	}
}

func TestDynamoDBStorage_List(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
	}

	prefix := "domain"

	fixturesWithPrefix := map[string]string{
		"domain1": "cert1",
		"domain2": "cert2",
		"domain3": "cert3",
	}
	for k, v := range fixturesWithPrefix {
		err := storage.Store(context.Background(), k, []byte(v))
		if err != nil {
			t.Errorf("failed to store fixture %s, error: %s", k, err.Error())
			return
		}
	}

	fixtures := map[string]string{
		"notinlist":        "cert4",
		"anothernotinlist": "cert5",
	}
	for k, v := range fixtures {
		err := storage.Store(context.Background(), k, []byte(v))
		if err != nil {
			t.Errorf("failed to store fixture %s, error: %s", k, err.Error())
			return
		}
	}

	foundKeys, err := storage.List(context.Background(), prefix, false)
	if err != nil {
		t.Errorf("failed to list keys: %s", err.Error())
		return
	}

	if len(foundKeys) != len(fixturesWithPrefix) {
		t.Errorf("did not get back expected number of keys, expected: %v, got: %v",
			len(fixturesWithPrefix), len(foundKeys))
		return
	}

	noKeysFound, err := storage.List(context.Background(), "invalid", false)
	if err != nil {
		t.Errorf("unable to list keys with invalid prefix: %s", err.Error())
		return
	}

	if len(noKeysFound) != 0 {
		t.Errorf("should not have found any keys but found %v key", len(noKeysFound))
		return
	}
}

func TestDynamoDBStorage_Stat(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
	}

	err = storage.Store(context.Background(), "key", []byte("value"))
	if err != nil {
		t.Errorf("failed to store fixture key/value: %s", err.Error())
		return
	}

	stat, err := storage.Stat(context.Background(), "key")
	if err != nil {
		t.Errorf("failed to stat item: %s", err.Error())
		return
	}

	if stat.Key != "key" {
		t.Errorf("stat key does not match expected. got: %s", stat.Key)
		return
	}
	if stat.Size != int64(len("value")) {
		t.Errorf("stat size does not match expected. got: %v", stat.Size)
		return
	}
	if time.Since(stat.Modified) > 5*time.Second {
		t.Errorf("stat modified time is not within 3 seoncds. got: %s", stat.Modified)
		return
	}
}

func TestDynamoDBStorage_Delete(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
	}

	err = storage.Store(context.Background(), "key", []byte("value"))
	if err != nil {
		t.Errorf("failed to store fixture key/value: %s", err.Error())
		return
	}

	value, err := storage.Load(context.Background(), "key")
	if err != nil {
		t.Errorf("unable to load key that was just stored: %s", err.Error())
		return
	}

	if string(value) != "value" {
		t.Errorf("value returned does not match expected. expected: %s, got: %s", "value", string(value))
		return
	}

	err = storage.Delete(context.Background(), "key")
	if err != nil {
		t.Errorf("unable to delete key: %s", err.Error())
		return
	}

	if storage.Exists(context.Background(), "key") {
		t.Errorf("key still exists after delete")
		return
	}
}

func TestDynamoDBStorage_LockConsistency(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	lockTimeout := 3 * time.Second
	lockPollingInterval := time.Second / 2 // should be less than lockTimeout
	lockRefreshInterval := 1 * time.Second // should be less than lockTimeout

	storage1 := Storage{
		Table:               TestTableName,
		AwsEndpoint:         os.Getenv("AWS_ENDPOINT"),
		AwsRegion:           os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL:       DisableSSL,
		LockTimeout:         caddy.Duration(lockTimeout),
		LockPollingInterval: caddy.Duration(lockPollingInterval),
		LockRefreshInterval: caddy.Duration(lockRefreshInterval),
	}
	storage2 := Storage{
		Table:               TestTableName,
		AwsEndpoint:         os.Getenv("AWS_ENDPOINT"),
		AwsRegion:           os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL:       DisableSSL,
		LockTimeout:         caddy.Duration(lockTimeout),
		LockPollingInterval: caddy.Duration(lockPollingInterval),
		LockRefreshInterval: caddy.Duration(lockRefreshInterval),
	}

	key := "test1"

	// create lock with first instance
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	err = storage1.Lock(ctx1, key)
	if err != nil {
		t.Errorf("error creating lock: %s", err.Error())
	}

	// try to create lock again with another instance,
	// but it should not be able to create lock until the first lock is released.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	err = storage2.Lock(ctx2, key) // this call should be cancelled after 2 seconds
	if err == nil {
		t.Errorf("another instance was able to create lock while it should not be able to until the first lock is released")
	}

	// release the lock
	err = storage1.Unlock(ctx1, key)
	if err != nil {
		t.Errorf("error releasing lock: %s", err.Error())
	}

	// try to create lock again with another instance, it should be able to create lock now
	ctx2, cancel2 = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	err = storage2.Lock(ctx2, key)
	if err != nil {
		t.Errorf("error creating lock second time: %s", err.Error())
	}
}

func TestDynamoDBStorage_StaleLock(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	lockTimeout := 1 * time.Second

	storage1 := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
		LockTimeout:   caddy.Duration(lockTimeout),
	}
	storage2 := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
		LockTimeout:   caddy.Duration(lockTimeout),
	}

	key := "test1"

	// create lock
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	err = storage1.Lock(ctx1, key)
	if err != nil {
		t.Errorf("error creating lock: %s", err.Error())
	}

	// emulate the first instance killed before unlocking by cancelling the context
	// to stop refreshing the lock.
	cancel1()

	before := time.Now()

	// try to create lock again with another instance,
	// it should take about 1-2 seconds to return
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	err = storage2.Lock(ctx2, key)
	if err != nil {
		t.Errorf("error creating lock second time: %s", err.Error())
	}
	if time.Since(before) < lockTimeout {
		t.Errorf("creating second lock finished quicker than it shoud, in %v seconds", time.Since(before).Seconds())
	}
}

func TestDynamoDBStorage_UnlockNonExistantKey(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	lockTimeout := 1 * time.Second

	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
		LockTimeout:   caddy.Duration(lockTimeout),
	}

	// try to unlock a key that doesn't exist
	err = storage.Unlock(context.TODO(), "doesntexist")
	if err != nil {
		t.Errorf("got error unlocking non-existant key")
	}
}

func TestDynamoDBStorage_LoadErrNotExist(t *testing.T) {
	err := initDb()
	if err != nil {
		t.Error(err)
		return
	}

	storage := Storage{
		Table:         TestTableName,
		AwsEndpoint:   os.Getenv("AWS_ENDPOINT"),
		AwsRegion:     os.Getenv("AWS_DEFAULT_REGION"),
		AwsDisableSSL: DisableSSL,
	}

	_, err = storage.Load(context.Background(), "notarealkey")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err was not a ErrNotExist, got: %s", err.Error())
	}
}
