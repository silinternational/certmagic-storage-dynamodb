package skydbstorage

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

func init() {
	caddy.RegisterModule(Storage{}) // TODO Make this a constructor
}

// CaddyModule returns the Caddy module information.
func (Storage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.skydb",
		New: func() caddy.Module { return new(Storage) },
	}
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (s *Storage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// UnmarshalCaddyfile sets up the storage module from Caddyfile tokens. Syntax:
//
// TODO: This needs to be adapted to SkyDB
// dynamodb <table_name> {
//     aws_endpoint <endpoint>
//     aws_region   <region>
// }
//
// skydb <base64_pubkey> {
// 		skydb_endpoint <endpoint> TODO Do we even need this?
// }
//
// Only the table name is required.
func (s *Storage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if !d.NextArg() {
			return d.ArgErr()
		}
		s.Table = d.Val()

		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "skydb_endpoint":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.SkyDBEndpoint = d.Val()
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
