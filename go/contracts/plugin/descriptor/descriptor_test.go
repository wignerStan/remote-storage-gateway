package descriptor

import (
	"bytes"
	"encoding/json"
	"testing"
)

func valid() Descriptor {
	return Descriptor{Schema: Schema, PluginID: "test.client", PluginVersion: "1.0.0", Contract: "network-data-plane-worker/client/v1", Runtime: "go", BuildDigest: "sha256:" + string(bytes.Repeat([]byte("a"), 64)), Implementations: []Implementation{{ID: "adapter", ConfigSchema: ConfigSchema{Version: 1, Fields: []Field{{Path: "endpoint", Type: "string", Required: true}, {Path: "password", Type: "string", Secret: true, WriteOnly: true}}}, Capabilities: []string{"stat", "open", "put"}}}}
}
func TestExactDigestAndSchemaValidation(t *testing.T) {
	d := valid()
	r, e := Encode(d)
	if e != nil {
		t.Fatal(e)
	}
	again, e := Encode(d)
	if e != nil || again.Digest != r.Digest {
		t.Fatal("unstable digest")
	}
	spaced := append([]byte(" "), r.Bytes...)
	v, e := Decode(spaced)
	if e != nil || v.Digest == r.Digest {
		t.Fatal("exact bytes were normalized")
	}
	r.Bytes[0] = '!'
	if again.Bytes[0] != '{' {
		t.Fatal("descriptor bytes aliased")
	}
	for _, b := range [][]byte{[]byte(`{"schema":"x","schema":"y"}`), []byte(`{"nested":{"a":1,"a":2}}`), append(append([]byte(nil), again.Bytes[:len(again.Bytes)-1]...), []byte(`,"unknown":true}`)...)} {
		if _, e := Decode(b); e == nil {
			t.Fatal("malformed descriptor admitted")
		}
	}
	d.Implementations[0].ConfigSchema.Fields[1].Default = json.RawMessage(`"plaintext"`)
	if _, e := Encode(d); e == nil {
		t.Fatal("secret default admitted")
	}
}
func TestPublicSecretSeparation(t *testing.T) {
	a := valid().Implementations[0]
	if e := ValidateConfig(a, []byte(`{"endpoint":"https://example.test"}`), map[string]string{"password": "secrets/password"}); e != nil {
		t.Fatal(e)
	}
	for _, r := range []struct {
		public string
		refs   map[string]string
	}{{`{"endpoint":"ok","password":"secret"}`, nil}, {`{"endpoint":"ok","extra":true}`, nil}, {`{"endpoint":"ok"}`, map[string]string{"undeclared": "secret/ref"}}, {`{}`, nil}, {`{"endpoint":1}`, nil}, {`{"endpoint":"a","endpoint":"b"}`, nil}} {
		if e := ValidateConfig(a, []byte(r.public), r.refs); e == nil {
			t.Fatalf("invalid public/secret config admitted: %s", r.public)
		}
	}
}
