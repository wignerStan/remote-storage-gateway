// Package descriptor owns exact-byte plugin descriptors and their bounded schema.
package descriptor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

const Schema = "network-data-plane-worker/plugin-descriptor/v1"
const MaxBytes = 1 << 20

var id = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
var digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func ValidDigest(v string) bool { return digest.MatchString(v) }

type Descriptor struct {
	Schema          string           `json:"schema"`
	PluginID        string           `json:"plugin_id"`
	PluginVersion   string           `json:"plugin_version"`
	Contract        string           `json:"contract"`
	Runtime         string           `json:"runtime"`
	BuildDigest     string           `json:"build_digest"`
	Implementations []Implementation `json:"implementations"`
}
type Implementation struct {
	ID           string       `json:"id"`
	DisplayName  string       `json:"display_name,omitempty"`
	ConfigSchema ConfigSchema `json:"config_schema"`
	Capabilities []string     `json:"capabilities"`
}
type ConfigSchema struct {
	Version int     `json:"version"`
	Fields  []Field `json:"fields"`
}
type Field struct {
	Path        string            `json:"path"`
	Type        string            `json:"type"`
	Required    bool              `json:"required,omitempty"`
	Secret      bool              `json:"secret,omitempty"`
	WriteOnly   bool              `json:"write_only,omitempty"`
	Default     json.RawMessage   `json:"default,omitempty"`
	Enum        []json.RawMessage `json:"enum,omitempty"`
	MinLength   *uint32           `json:"min_length,omitempty"`
	MaxLength   *uint32           `json:"max_length,omitempty"`
	Minimum     *int64            `json:"minimum,omitempty"`
	Maximum     *int64            `json:"maximum,omitempty"`
	Description string            `json:"description,omitempty"`
}
type Record struct {
	Descriptor Descriptor
	Bytes      []byte
	Digest     string
}

func StrictJSON(b []byte, v any) error {
	if len(b) == 0 || len(b) > MaxBytes {
		return errors.New("JSON frame size exceeded")
	}
	// Duplicate keys are rejected before decoding; last-value wins would make
	// approval depend on which implementation interpreted the same bytes.
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := uniqueValue(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	d.UseNumber()
	return d.Decode(v)
}
func uniqueValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting exceeded")
	}
	t, e := d.Token()
	if e != nil {
		return e
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return e
			}
			s, ok := k.(string)
			if !ok || seen[s] {
				return errors.New("duplicate JSON field")
			}
			seen[s] = true
			if e = uniqueValue(d, depth+1); e != nil {
				return e
			}
		}
		_, e = d.Token()
		return e
	case '[':
		for d.More() {
			if e = uniqueValue(d, depth+1); e != nil {
				return e
			}
		}
		_, e = d.Token()
		return e
	default:
		return errors.New("invalid JSON delimiter")
	}
}
func Encode(d Descriptor) (Record, error) {
	b, e := json.Marshal(d)
	if e != nil {
		return Record{}, e
	}
	return Decode(b)
}
func Decode(b []byte) (Record, error) {
	var d Descriptor
	if e := StrictJSON(b, &d); e != nil {
		return Record{}, e
	}
	if e := d.Validate(); e != nil {
		return Record{}, e
	}
	sum := sha256.Sum256(b)
	return Record{d, bytes.Clone(b), "sha256:" + hex.EncodeToString(sum[:])}, nil
}
func (d Descriptor) Validate() error {
	if d.Schema != Schema || !id.MatchString(d.PluginID) || !id.MatchString(d.PluginVersion) || !ValidDigest(d.BuildDigest) || (d.Runtime != "go" && d.Runtime != "rust") {
		return errors.New("invalid descriptor identity")
	}
	switch d.Contract {
	case "network-data-plane-worker/client/v1", "network-data-plane-worker/l4-policy/v1", "network-data-plane-worker/l7-policy/v1", "network-data-plane-worker/protocol/v1":
	default:
		return errors.New("unsupported plugin contract")
	}
	if len(d.Implementations) == 0 || len(d.Implementations) > 256 {
		return errors.New("invalid implementation inventory")
	}
	seen := map[string]bool{}
	for _, a := range d.Implementations {
		if !id.MatchString(a.ID) || seen[a.ID] || len(a.DisplayName) > 512 || a.ConfigSchema.Version != 1 || len(a.ConfigSchema.Fields) > 256 {
			return errors.New("invalid implementation schema")
		}
		seen[a.ID] = true
		fields := map[string]bool{}
		for _, f := range a.ConfigSchema.Fields {
			if !id.MatchString(f.Path) || bytes.ContainsRune([]byte(f.Path), '.') || fields[f.Path] || len(f.Description) > 4096 {
				return errors.New("invalid or duplicate field path")
			}
			fields[f.Path] = true
			switch f.Type {
			case "string", "boolean", "integer", "number", "object", "array":
			default:
				return errors.New("unknown field type")
			}
			if f.Secret && (!f.WriteOnly || len(f.Default) > 0 || len(f.Enum) > 0) {
				return errors.New("secret field must be write-only and have no defaults/enumeration")
			}
			if f.MinLength != nil && f.MaxLength != nil && *f.MinLength > *f.MaxLength || f.Minimum != nil && f.Maximum != nil && *f.Minimum > *f.Maximum {
				return errors.New("invalid field bounds")
			}
			if len(f.Enum) > 256 {
				return errors.New("enum size exceeded")
			}
			if len(f.Default) > 0 {
				if e := validateValue(f, f.Default); e != nil {
					return fmt.Errorf("invalid field default: %w", e)
				}
			}
			for _, v := range f.Enum {
				if e := validateValue(Field{Type: f.Type}, v); e != nil {
					return errors.New("invalid enumeration")
				}
			}
		}
		caps := map[string]bool{}
		if len(a.Capabilities) == 0 || len(a.Capabilities) > 64 {
			return errors.New("invalid capabilities")
		}
		for _, c := range a.Capabilities {
			if !id.MatchString(c) || caps[c] {
				return errors.New("invalid capability")
			}
			caps[c] = true
		}
	}
	return nil
}
func (d Descriptor) Implementation(id string) (Implementation, bool) {
	for _, v := range d.Implementations {
		if v.ID == id {
			return v, true
		}
	}
	return Implementation{}, false
}
