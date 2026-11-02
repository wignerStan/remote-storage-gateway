// Package admission owns bounded descriptor parsing and configuration admission.
package admission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/wignerStan/infra/go/config/codec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"regexp"
)

var id = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

// StrictJSON applies the Worker's metadata frame bound and the canonical
// configuration codec's duplicate, exact-key, UTF-8 and shape guarantees.
func StrictJSON(b []byte, value any) error {
	if len(b) == 0 || len(b) > descriptor.MaxBytes {
		return errors.New("JSON frame size exceeded")
	}
	return codec.DecodeJSONValue(b, value)
}
func Encode(d descriptor.Descriptor) (descriptor.Record, error) {
	b, e := json.Marshal(d)
	if e != nil {
		return descriptor.Record{}, e
	}
	return Decode(b)
}
func Decode(b []byte) (descriptor.Record, error) {
	var d descriptor.Descriptor
	if e := StrictJSON(b, &d); e != nil {
		return descriptor.Record{}, e
	}
	if e := Validate(d); e != nil {
		return descriptor.Record{}, e
	}
	sum := sha256.Sum256(b)
	return descriptor.Record{Descriptor: d, Bytes: bytes.Clone(b), Digest: "sha256:" + hex.EncodeToString(sum[:])}, nil
}
func Validate(d descriptor.Descriptor) error {
	if d.Schema != descriptor.Schema || !id.MatchString(d.PluginID) || !id.MatchString(d.PluginVersion) || !descriptor.ValidDigest(d.BuildDigest) || (d.Runtime != "go" && d.Runtime != "rust") {
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
		if !id.MatchString(a.ID) || seen[a.ID] || len(a.DisplayName) > 512 {
			return errors.New("invalid implementation schema")
		}
		seen[a.ID] = true
		if e := validateSchema(a.ConfigSchema); e != nil {
			return e
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
