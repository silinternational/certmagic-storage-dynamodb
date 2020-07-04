package dynamodbstorage

import (
	caddy "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

func init() {
	caddy.RegisterModule(Storage{})
}

// CaddyModule returns the Caddy module information.
func (Storage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.dynamodb",
		New: func() caddy.Module { return new(Storage) },
	}
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (s *Storage) CertMagicStorage() (certmagic.Storage, error) {
	return s, nil
}

// UnmarshalCaddyfile sets up the storage module from Caddyfile tokens. Syntax:
//
// dynamodb <table_name> {
//     aws_endpoint <endpoint>
//     aws_region   <region>
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
			case "aws_endpoint":
				if !d.NextArg() {
					return d.ArgErr()
				}
				s.AwsEndpoint = d.Val()
			case "aws_region":
				s.AwsRegion = d.Val()
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
