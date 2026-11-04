package admission

import (
	"bytes"
	"encoding/json"
	"github.com/wignerStan/infra/go/code/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"testing"
)

type publicConfig struct {
	Endpoint string `json:"endpoint"`
	Password string `json:"password,omitempty" writeOnly:"true"`
}

func generatedConfig[T any]() descriptor.ConfigSchema {
	doc, err := schema.Derive[T](schema.Identity{ID: "worker.test.config", Version: "1", Owner: "network-data-plane-worker"})
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(doc.Schema)
	if err != nil {
		panic(err)
	}
	return b
}

func valid() descriptor.Descriptor {
	return descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.client", PluginVersion: "1.0.0", Contract: "network-data-plane-worker/client/v1", Runtime: "go", BuildDigest: "sha256:" + string(bytes.Repeat([]byte("a"), 64)), Implementations: []descriptor.Implementation{{ID: "adapter", ConfigSchema: generatedConfig[publicConfig](), Capabilities: []string{"stat", "open", "put"}}}}
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
	legacy := bytes.Replace(again.Bytes, []byte(descriptor.Schema), []byte("network-data-plane-worker/plugin-descriptor/v1"), 1)
	if _, err := Decode(legacy); err == nil {
		t.Fatal("legacy envelope was accepted without reapproval")
	}
	d.Implementations[0].ConfigSchema = bytes.Replace(d.Implementations[0].ConfigSchema, []byte(`"writeOnly":true`), []byte(`"writeOnly":true,"default":"plaintext"`), 1)
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
