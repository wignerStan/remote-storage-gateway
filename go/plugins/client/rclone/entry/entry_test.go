package rclone_test

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/wignerStan/infra/go/config/codec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/client/rclone"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/client/rclone/entry"
)

const buildDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func published(t *testing.T) descriptor.Descriptor {
	t.Helper()
	factory, err := rclone.NewFactory(buildDigest)
	if err != nil {
		t.Fatal(err)
	}
	value := factory.Descriptor()
	if err := admission.Validate(value); err != nil {
		t.Fatalf("entry published an invalid descriptor: %v", err)
	}
	return value
}

func TestEntryDerivesConcreteConfigurationSchemas(t *testing.T) {
	value := published(t)
	cases := []struct {
		id       string
		config   any
		required string
		secrets  []string
	}{
		{"s3", rcloneclient.S3Configuration{S3PublicConfig: rcloneclient.S3PublicConfig{Region: "fixture-region", Bucket: "fixture-bucket"}, AccessKeyID: "fixture-access", SecretAccessKey: "fixture-secret"}, "region", []string{"access_key_id", "secret_access_key", "session_token"}},
		{"webdav", rcloneclient.WebDAVConfiguration{WebDAVPublicConfig: rcloneclient.WebDAVPublicConfig{URL: "https://fixture.invalid/dav/"}}, "url", []string{"password"}},
		{"webdav-streaming", rcloneclient.WebDAVConfiguration{WebDAVPublicConfig: rcloneclient.WebDAVPublicConfig{URL: "https://fixture.invalid/dav/"}}, "url", []string{"password"}},
		{"drive", rcloneclient.DriveConfiguration{Token: "{}"}, "token", []string{"token", "client_secret"}},
	}
	if len(value.Implementations) != len(cases) || value.PluginID != rcloneclient.PluginID || value.BuildDigest != buildDigest {
		t.Fatalf("unexpected entry identity: %+v", value)
	}
	for _, test := range cases {
		t.Run(test.id, func(t *testing.T) {
			implementation, ok := value.Implementation(test.id)
			if !ok {
				t.Fatal("implementation not published")
			}
			compiled, err := codec.CompileJSONSchema(implementation.ConfigSchema)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(test.config)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(raw); err != nil {
				t.Fatalf("derived schema rejects its concrete configuration: %v", err)
			}
			var document struct {
				Type       string `json:"type"`
				Properties map[string]struct {
					WriteOnly bool `json:"writeOnly"`
				} `json:"properties"`
			}
			if err := json.Unmarshal(implementation.ConfigSchema, &document); err != nil || document.Type != "object" {
				t.Fatalf("configuration schema must expose its object root: %v", err)
			}
			for _, name := range test.secrets {
				if !document.Properties[name].WriteOnly {
					t.Errorf("implementation-owned secret %q is not writeOnly", name)
				}
			}
			var input map[string]json.RawMessage
			if err := json.Unmarshal(raw, &input); err != nil {
				t.Fatal(err)
			}
			delete(input, test.required)
			missing, _ := json.Marshal(input)
			if err := compiled.Validate(missing); err == nil {
				t.Fatalf("missing required field %q was accepted", test.required)
			}
		})
	}
}

func TestEntryDistinguishesOrdinaryAndStreamingWebDAV(t *testing.T) {
	value := published(t)
	for _, implementation := range value.Implementations {
		has := slices.Contains(implementation.Capabilities, "unknown-size-put")
		if has != (implementation.ID != "webdav") {
			t.Errorf("%s unknown-size-put=%v", implementation.ID, has)
		}
	}
}

func TestRuntimeFactoryFreezesPublishedDescriptor(t *testing.T) {
	value := published(t)
	before, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	factory := rcloneclient.NewFactory(value)
	mutate := func(value *descriptor.Descriptor) {
		value.PluginID = "fixture.changed"
		value.Implementations[0].ID = "fixture.changed"
		value.Implementations[0].ConfigSchema[0] = '!'
		value.Implementations[0].Capabilities[0] = "fixture.changed"
	}
	assertUnchanged := func() {
		t.Helper()
		after, err := json.Marshal(factory.Descriptor())
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("descriptor escaped its immutable publication snapshot: %v", err)
		}
	}
	mutate(&value)
	assertUnchanged()
	returned := factory.Descriptor()
	mutate(&returned)
	assertUnchanged()
}

func TestEntryRejectsInvalidPublicationBeforeFactoryRegistration(t *testing.T) {
	factory, err := rclone.NewFactory("invalid-build-digest")
	if err == nil || factory != nil {
		t.Fatal("invalid publication metadata exposed a runtime factory")
	}
}
