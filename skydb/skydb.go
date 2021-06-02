package skydb

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"os"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/node/api/client"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// SkyDBI is the interface for communicating with SkyDB. We use an interface, so
// we can easily override it for testing purposes.
type SkyDBI interface {
	Read(crypto.Hash) ([]byte, uint64, error)
	Write(data []byte, dataKey crypto.Hash, rev uint64) error
}

type SkyDB struct {
	Client *client.Client
	sk     crypto.SecretKey
	pk     crypto.PublicKey
}

// New creates a new SkyDB client with default option.
// The default options are to look for a skyd node at localhost:9980,
// to use "Sia-Agent" as user agent and to get the password for skyd from the
// environment.
func New() (*SkyDB, error) {
	sk, err1 := base64.StdEncoding.DecodeString(os.Getenv("SKYDB_SEC_KEY"))
	pk, err2 := base64.StdEncoding.DecodeString(os.Getenv("SKYDB_PUB_KEY"))
	if err1 != nil || err2 != nil {
		return nil, errors.AddContext(errors.Compose(err1, err2), "failed to decode SKYDB_SEC_KEY and/or SKYDB_PUB_KEY")
	}
	if len(sk) == 0 || len(pk) == 0 {
		return nil, errors.New("missing SKYDB_SEC_KEY or SKYDB_PUB_KEY environment variable")
	}
	opts, err := client.DefaultOptions()
	if err != nil {
		return nil, errors.AddContext(err, "failed to get default client options")
	}
	skydb := &SkyDB{Client: &client.Client{opts}}
	copy(skydb.sk[:], sk)
	copy(skydb.pk[:], pk)
	return skydb, nil
}

// NewCustom creates a new SkyDB client with the given options.
func NewCustom(opts client.Options, sk crypto.SecretKey, pk crypto.PublicKey) *SkyDB {
	skydb := &SkyDB{Client: &client.Client{opts}}
	skydb.sk = sk
	skydb.pk = pk
	return skydb
}

// Read retrieves from SkyDB the data that corresponds to the given key set.
func (db SkyDB) Read(dataKey crypto.Hash) ([]byte, uint64, error) {
	s, rev, err := registryRead(db.Client, db.pk, dataKey)
	if err != nil {
		return nil, 0, errors.AddContext(err, "failed to read from registry")
	}
	b, err := db.Client.SkynetSkylinkGet(s.String())
	if err != nil {
		return nil, 0, errors.AddContext(err, "failed to download data from Skynet")
	}
	return b, rev, nil
}

// Write stores the given `data` in SkyDB under the given key set.
func (db SkyDB) Write(data []byte, dataKey crypto.Hash, rev uint64) error {
	skylink, err := uploadData(db.Client, data)
	if err != nil {
		return errors.AddContext(err, "failed to upload data")
	}
	_, err = registryWrite(db.Client, skylink, db.sk, db.pk, dataKey, rev)
	if err != nil {
		return errors.AddContext(err, "failed to write to the registry")
	}
	return nil
}

// registryWrite updates the registry entry with the given dataKey to contain the
// given skylink. Returns a SkylinkV2.
func registryWrite(c *client.Client, skylink string, sk crypto.SecretKey, pk crypto.PublicKey, dataKey crypto.Hash, rev uint64) (skymodules.Skylink, error) {
	var sl skymodules.Skylink
	err := sl.LoadString(skylink)
	if err != nil {
		return skymodules.Skylink{}, errors.AddContext(err, "failed to load skylink data")
	}
	// Update the registry with that link.
	spk := types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       pk[:],
	}
	srv := modules.NewRegistryValue(dataKey, sl.Bytes(), rev).Sign(sk)
	err = c.RegistryUpdate(spk, dataKey, srv.Revision, srv.Signature, sl)
	if err != nil {
		return skymodules.Skylink{}, err
	}
	return skymodules.NewSkylinkV2(spk, dataKey), nil
}

// registryRead reads a registry entry and returns the SkylinkV2 it contains, as well
// as the revision.
func registryRead(c *client.Client, pk crypto.PublicKey, dataKey crypto.Hash) (skymodules.Skylink, uint64, error) {
	spk := types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       pk[:],
	}

	srv, err := c.RegistryRead(spk, dataKey)
	if err != nil {
		return skymodules.Skylink{}, 0, errors.AddContext(err, "failed to read from the registry")
	}
	err = srv.Verify(pk)
	if err != nil {
		return skymodules.Skylink{}, 0, errors.AddContext(err, "the value we read failed validation")
	}
	var sl skymodules.Skylink
	err = sl.LoadBytes(srv.Data)
	if err != nil {
		return skymodules.Skylink{}, 0, errors.AddContext(err, "registry value is not a SkylinkV2")
	}
	return sl, srv.Revision, nil
}

// uploadData uploads the given data to skynet and returns a SkylinkV1.
func uploadData(c *client.Client, content []byte) (string, error) {
	buf := bytes.NewReader(content)
	fNameBytes := sha1.Sum(content)
	fName := base64.StdEncoding.EncodeToString(fNameBytes[:])
	sp, err := skymodules.NewSiaPath(fName)
	if err != nil {
		return "", errors.AddContext(err, "failed to create new sia path")
	}
	sup := &skymodules.SkyfileUploadParameters{
		SiaPath:  sp,
		Filename: fName,
		Force:    true,
		Mode:     skymodules.DefaultFilePerm,
		Reader:   buf,
	}
	skylink, _, err := c.SkynetSkyfilePost(*sup)
	if err != nil {
		return "", errors.AddContext(err, "failed to upload")
	}
	return skylink, nil
}
