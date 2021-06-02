package skydbstorage

import (
	"context"
	"github.com/skynetlabs/certmagic-storage-skydb/skydb"
	"gitlab.com/NebulousLabs/fastrand"
	"go.sia.tech/siad/crypto"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

type SkyDBRecord struct {
	Data     []byte
	Revision uint64
}

type SkyDBTest struct {
	store map[crypto.Hash]SkyDBRecord
}

func (db *SkyDBTest) Read(dataKey crypto.Hash) ([]byte, uint64, error) {
	if db.store == nil {
		db.store = make(map[crypto.Hash]SkyDBRecord)
	}
	rec, exists := db.store[dataKey]
	if !exists {
		return []byte{}, 0, nil // TODO Verify that this is the correct behaviour
	}
	return rec.Data, rec.Revision, nil
}

func (db *SkyDBTest) Write(data []byte, dataKey crypto.Hash, rev uint64) error {
	if db.store == nil {
		db.store = make(map[crypto.Hash]SkyDBRecord)
	}
	db.store[dataKey] = SkyDBRecord{
		Data:     data,
		Revision: rev,
	}
	return nil
}

func initTestStorage() (*Storage, error) {
	var testKeyListDataKey crypto.Hash
	fastrand.Read(testKeyListDataKey[:])
	var skyDbTest skydb.SkyDBI = &SkyDBTest{}
	return &Storage{
		SkyDB:               skyDbTest,
		LockTimeout:         0,
		LockPollingInterval: 0,
		KeyListDataKey:      testKeyListDataKey,
	}, nil
}

func TestSkyDBStorage_Store(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	type args struct {
		key   string
		value []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "simple key/value store",
			args: args{
				key:   "simple-key",
				value: []byte("value"),
			},
			wantErr: false,
		},
		{
			name: "empty key should error",
			args: args{
				key:   "",
				value: []byte("value"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = storage.Store(tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Store() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				loaded, err := storage.Load(tt.args.key)
				if err != nil {
					t.Errorf("failed to load after store: %storage", err.Error())
					return
				}
				if string(loaded) != string(tt.args.value) {
					t.Errorf("Load() returned value other than expected. Expected: %storage, Actual: %storage", string(tt.args.value), string(loaded))
					return
				}
			}
		})
	}
}

func TestSkyDBStorage_List(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	prefix := "domain"

	fixturesWithPrefix := map[string]string{
		"domain1": "cert1",
		"domain2": "cert2",
		"domain3": "cert3",
	}
	for k, v := range fixturesWithPrefix {
		err := storage.Store(k, []byte(v))
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
		err := storage.Store(k, []byte(v))
		if err != nil {
			t.Errorf("failed to store fixture %s, error: %s", k, err.Error())
			return
		}
	}

	foundKeys, err := storage.List(prefix, false)
	if err != nil {
		t.Errorf("failed to list keys: %s", err.Error())
		return
	}

	if len(foundKeys) != len(fixturesWithPrefix) {
		t.Errorf("did not get back expected number of keys, expected: %v, got: %v",
			len(fixturesWithPrefix), len(foundKeys))
		return
	}

	noKeysFound, err := storage.List("invalid", false)
	if err != nil {
		t.Errorf("unable to list keys with invalid prefix: %s", err.Error())
		return
	}

	if len(noKeysFound) != 0 {
		t.Errorf("should not have found any keys but found %v key", len(noKeysFound))
		return
	}
}

func TestSkyDBStorage_Stat(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	err = storage.Store("key", []byte("value"))
	if err != nil {
		t.Errorf("failed to store fixture key/value: %s", err.Error())
		return
	}

	stat, err := storage.Stat("key")
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

func TestSkyDBStorage_Delete(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	err = storage.Store("key", []byte("value"))
	if err != nil {
		t.Errorf("failed to store fixture key/value: %s", err.Error())
		return
	}

	value, err := storage.Load("key")
	if err != nil {
		t.Errorf("unable to load key that was just stored: %s", err.Error())
		return
	}

	if string(value) != "value" {
		t.Errorf("value returned does not match expected. expected: %s, got: %s", "value", string(value))
		return
	}

	err = storage.Delete("key")
	if err != nil {
		t.Errorf("unable to delete key: %s", err.Error())
		return
	}

	if storage.Exists("key") {
		t.Errorf("key still exists after delete")
		return
	}
}

func TestSkyDBStorage_Lock(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	lockTimeout := 1 * time.Second
	storage.LockTimeout = caddy.Duration(lockTimeout)

	// create lock
	key := "test1"
	err = storage.Lock(context.TODO(), key)
	if err != nil {
		t.Errorf("error creating lock: %s", err.Error())
	}

	// try to create lock again, it should take about 1-2 seconds to return
	before := time.Now()
	err = storage.Lock(context.TODO(), key)
	if err != nil {
		t.Errorf("error creating lock second time: %s", err.Error())
	}
	if time.Since(before) < lockTimeout {
		t.Errorf("creating second lock finished quicker than it should, in %v seconds", time.Since(before).Seconds())
	}

	// try to unlock a key that doesn't exist
	err = storage.Unlock("doesntexist")
	if err != nil {
		t.Errorf("got error unlocking non-existant key")
	}
}

func TestSkyDBStorage_LoadErrNotExist(t *testing.T) {
	storage, err := initTestStorage()
	if err != nil {
		t.Error(err)
		return
	}

	_, err = storage.Load("notarealkey")
	_, isNotErrNotExist := err.(certmagic.ErrNotExist)
	if !isNotErrNotExist {
		t.Errorf("err was not a ErrNotExist, got: %s", err.Error())
	}
}
