package broker

import (
	"net/url"
	"strings"
)

// ValidateIntent admits only finite method and logical routing metadata. An
// empty configured name set admits no metadata; it never means all headers.
func ValidateIntent(in TransferIntent, allowedMetadata map[string]bool) error {
	if !validMethod(in.Method) || !safeText(in.Authority, 256) || strings.ContainsAny(in.Authority, "/@?#\\") || !optionalID(in.SourceID) || !optionalID(in.TargetID) || len(in.Metadata) > MaxMetadataEntries {
		return ErrInvalid
	}
	size := 0
	for key, value := range in.Metadata {
		if key != strings.ToLower(key) || !allowedMetadata[key] || forbiddenMetadata(key) || !safeText(key, 128) || !safeText(value, 1024) {
			return ErrInvalid
		}
		size += len(key) + len(value)
		if size > MaxMetadataBytes {
			return ErrInvalid
		}
	}
	return nil
}

// ValidateDecision fails closed for expired/unbounded grants, wildcard methods,
// addresses carrying credentials and provider URLs in place of logical IDs.
func ValidateDecision(in TransferIntent, d Decision) error {
	if !validID(d.RouteID) || !validID(d.ApprovedUpstreamID) || d.TTLSeconds <= 0 || d.TTLSeconds > MaxTTLSeconds || d.MaxMessageBytes <= 0 || d.MaxMessageBytes > MaxMessageBytes || d.MaxInflightBytes < d.MaxMessageBytes || d.MaxInflightBytes > MaxInflightBytes || len(d.Methods) == 0 || len(d.Methods) > 16 {
		return ErrInvalid
	}
	u, err := url.Parse(d.WorkerDataEndpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return ErrInvalid
	}
	found := false
	seen := map[string]bool{}
	for _, method := range d.Methods {
		if !validMethod(method) || seen[method] {
			return ErrInvalid
		}
		seen[method] = true
		found = found || method == in.Method
	}
	if !found {
		return ErrDenied
	}
	return nil
}

func forbiddenMetadata(key string) bool {
	switch key {
	case "authorization", "proxy-authorization", "cookie", "set-cookie":
		return true
	}
	return false
}
func safeText(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range s {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}
func optionalID(s string) bool { return s == "" || validID(s) }
func validID(s string) bool {
	if !safeText(s, 128) {
		return false
	}
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}
func validMethod(s string) bool {
	if len(s) > 512 || !strings.HasPrefix(s, "/") || strings.Count(s, "/") != 2 {
		return false
	}
	parts := strings.Split(s[1:], "/")
	for n, p := range parts {
		if p == "" {
			return false
		}
		for _, c := range p {
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || (n == 0 && c == '.') {
				continue
			}
			return false
		}
	}
	return true
}
