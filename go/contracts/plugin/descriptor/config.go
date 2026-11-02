package descriptor

import (
	"bytes"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

func validateValue(f Field, b []byte) error {
	var v any
	if e := StrictJSON(b, &v); e != nil {
		return e
	}
	good := false
	switch f.Type {
	case "string":
		s, ok := v.(string)
		good = ok
		if ok {
			n := uint32(utf8.RuneCountInString(s))
			if f.MinLength != nil && n < *f.MinLength || f.MaxLength != nil && n > *f.MaxLength {
				return errors.New("string bound violated")
			}
		}
	case "boolean":
		_, good = v.(bool)
	case "integer":
		n, ok := v.(json.Number)
		good = ok
		if ok {
			x, e := n.Int64()
			if e != nil {
				return errors.New("integer expected")
			}
			if f.Minimum != nil && x < *f.Minimum || f.Maximum != nil && x > *f.Maximum {
				return errors.New("integer bound violated")
			}
		}
	case "number":
		_, good = v.(json.Number)
	case "object":
		_, good = v.(map[string]any)
	case "array":
		_, good = v.([]any)
	}
	if !good {
		return errors.New("field type mismatch")
	}
	if len(f.Enum) > 0 {
		canonical, _ := json.Marshal(v)
		found := false
		for _, x := range f.Enum {
			var y any
			_ = json.Unmarshal(x, &y)
			z, _ := json.Marshal(y)
			if bytes.Equal(z, canonical) {
				found = true
			}
		}
		if !found {
			return errors.New("field not in enumeration")
		}
	}
	return nil
}

// ValidateConfig separates declared secret references from public values. It is
// structural only; provider constructors still perform final typed validation.
func ValidateConfig(a Implementation, public json.RawMessage, secrets map[string]string) error {
	if len(public) == 0 {
		public = json.RawMessage(`{}`)
	}
	var values map[string]json.RawMessage
	if e := StrictJSON(public, &values); e != nil {
		return e
	}
	if values == nil {
		return errors.New("public configuration must be an object")
	}
	fields := map[string]Field{}
	for _, f := range a.ConfigSchema.Fields {
		fields[f.Path] = f
	}
	for k, v := range values {
		f, ok := fields[k]
		if !ok || f.Secret {
			return errors.New("undeclared or secret public field")
		}
		if e := validateValue(f, v); e != nil {
			return e
		}
	}
	for k, ref := range secrets {
		f, ok := fields[k]
		if !ok || !f.Secret || len(ref) == 0 || len(ref) > 4096 {
			return errors.New("undeclared secret reference")
		}
	}
	for _, f := range a.ConfigSchema.Fields {
		if !f.Required {
			continue
		}
		if f.Secret {
			if secrets[f.Path] == "" {
				return errors.New("required secret missing")
			}
		} else if _, ok := values[f.Path]; !ok && len(f.Default) == 0 {
			return errors.New("required public field missing")
		}
	}
	return nil
}
