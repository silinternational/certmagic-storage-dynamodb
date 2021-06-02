package skydbstorage

import (
	"encoding/base64"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

func init() {
	storage, err := NewStorage() // TODO Handle error
	if err != nil {
		panic(err)
	}
	caddy.RegisterModule(storage)
}

// CaddyModule returns the Caddy module information.
func (Storage) CaddyModule() caddy.ModuleInfo {
	storage, err := NewStorage() // TODO Handle error
	if err != nil {
		panic(err)
	}
	return caddy.ModuleInfo{
		ID:  "caddy.storage.skydb",
		New: func() caddy.Module { return storage },
	}
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (s *Storage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// UnmarshalCaddyfile sets up the storage module from Caddyfile tokens. Syntax:
//
// skydb {
// 		key_list_datakey <base64 encoded dataKey>
// }
//
// Optional. If not provided, the hardcoded key will be used.
func (s *Storage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if !d.NextArg() {
			return d.ArgErr()
		}
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "key_list_datakey":
				if !d.NextArg() {
					return d.ArgErr()
				}
				dk, err := base64.StdEncoding.DecodeString(d.Val())
				if err != nil {
					return d.Errf("failed to decode key list dataKey. Error: %v", err)
				}
				if len(dk) != len(s.KeyListDataKey) {
					return d.Errf("bad size of key list dataKey. Expected %d, got %d.", len(s.KeyListDataKey), len(dk))
				}
				copy(s.KeyListDataKey[:], dk)
			default:
				return d.Errf("unrecognized parameter '%s'", d.Val())
			}
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.StorageConverter = (*Storage)(nil)
	_ caddyfile.Unmarshaler  = (*Storage)(nil)
)
