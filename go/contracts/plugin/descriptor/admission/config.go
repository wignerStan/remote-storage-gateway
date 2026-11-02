package admission

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/wignerStan/infra/go/config/codec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
)

// SecretField projects Worker secret semantics from standard writeOnly metadata.
// It does not describe a configuration type or provider validation vocabulary.
type SecretField struct {
	Name     string
	Required bool
}

func schemaObject(raw json.RawMessage) (map[string]any, error) {
	var out map[string]any
	if StrictJSON(raw, &out) != nil || out == nil || out["type"] != "object" {
		return nil, errors.New("configuration schema must be a standard object schema")
	}
	return out, nil
}

func validateSchema(raw json.RawMessage) error {
	root, err := schemaObject(raw)
	if err != nil {
		return err
	}
	properties, _ := root["properties"].(map[string]any)
	if len(properties) > 256 {
		return errors.New("configuration property limit exceeded")
	}
	// References address top-level secrets. Refuse nested declarations rather
	// than letting a secret be submitted through public configuration.
	var walk func(any, []string) error
	walk = func(value any, path []string) error {
		if len(path) > 32 {
			return errors.New("configuration schema nesting exceeded")
		}
		switch v := value.(type) {
		case map[string]any:
			if v["writeOnly"] == true {
				if len(path) != 2 || path[0] != "properties" || v["type"] != "string" {
					return errors.New("secret must be a top-level string property")
				}
				for _, key := range []string{"default", "enum", "examples", "const"} {
					if _, ok := v[key]; ok {
						return errors.New("secret schema cannot publish credential values")
					}
				}
			}
			for name, child := range v {
				if err := walk(child, append(path, name)); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range v {
				if err := walk(child, append(path, "[]")); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root, nil); err != nil {
		return err
	}
	if _, err := codec.CompileJSONSchema(raw); err != nil {
		return errors.New("invalid standard configuration schema")
	}
	return nil
}

func SecretFields(a descriptor.Implementation) []SecretField {
	root, err := schemaObject(a.ConfigSchema)
	if err != nil {
		return nil
	}
	required := map[string]bool{}
	if values, ok := root["required"].([]any); ok {
		for _, value := range values {
			if name, ok := value.(string); ok {
				required[name] = true
			}
		}
	}
	properties, _ := root["properties"].(map[string]any)
	out := []SecretField{}
	for name, value := range properties {
		if property, ok := value.(map[string]any); ok && property["writeOnly"] == true {
			out = append(out, SecretField{Name: name, Required: required[name]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ValidateConfig delegates structural checks to the standard-schema adapter.
// Secret references prove presence only; the provider's typed constructor
// remains responsible for final credential validation.
func ValidateConfig(a descriptor.Implementation, public json.RawMessage, refs map[string]string) error {
	if err := validateSchema(a.ConfigSchema); err != nil {
		return err
	}
	root, err := schemaObject(a.ConfigSchema)
	if err != nil {
		return err
	}
	if len(public) == 0 {
		public = json.RawMessage(`{}`)
	}
	var values map[string]any
	if StrictJSON(public, &values) != nil || values == nil {
		return errors.New("public configuration must be an object")
	}
	secrets := map[string]bool{}
	properties, _ := root["properties"].(map[string]any)
	for _, field := range SecretFields(a) {
		secrets[field.Name] = true
		if _, exists := values[field.Name]; exists {
			return errors.New("secret value in public configuration")
		}
		if field.Required && refs[field.Name] == "" {
			return errors.New("required secret reference missing")
		}
		properties[field.Name] = map[string]any{"type": "string"}
	}
	for name, ref := range refs {
		if !secrets[name] || len(ref) == 0 || len(ref) > 4096 {
			return errors.New("undeclared secret reference")
		}
		values[name] = "reference-present"
	}
	schema, err := json.Marshal(root)
	if err != nil {
		return errors.New("configuration schema unavailable")
	}
	value, err := json.Marshal(values)
	if err != nil {
		return errors.New("public configuration unavailable")
	}
	if codec.ValidateJSONSchema(schema, value) != nil {
		return errors.New("public configuration violates generated schema")
	}
	return nil
}
