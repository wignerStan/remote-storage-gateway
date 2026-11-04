package main

import (
	"path/filepath"
	"testing"
)

func TestNativeRecipeRejectsUncapturedInputs(t *testing.T) {
	include := filepath.Join(t.TempDir(), "canonical-include")
	allowed := map[string]string{
		"CGO_CFLAGS":   "-I" + include + " -O2 -g",
		"CGO_CPPFLAGS": "",
		"CGO_CXXFLAGS": "-O2 -g",
		"CGO_LDFLAGS":  "-O2 -g -pthread",
	}
	if err := validateNativeFlags(allowed, include); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, key, value string }{
		{"header-search", "CGO_CFLAGS", "-I/uncaptured/include"},
		{"forced-header", "CGO_CPPFLAGS", "-include /uncaptured/header.h"},
		{"compiler-plugin", "CGO_CXXFLAGS", "-fplugin=/uncaptured/plugin.so"},
		{"archive", "CGO_LDFLAGS", "/uncaptured/library.a"},
		{"library-search", "CGO_LDFLAGS", "-L/uncaptured/lib -lforeign"},
		{"response-file", "CGO_LDFLAGS", "-Wl,@/uncaptured/flags"},
		{"linker-script", "CGO_LDFLAGS", "-Wl,-T,/uncaptured/layout.ld"},
		{"include-as-link", "CGO_LDFLAGS", "-I" + include},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := make(map[string]string, len(allowed))
			for key, value := range allowed {
				settings[key] = value
			}
			settings[tc.key] = tc.value
			if err := validateNativeFlags(settings, include); err == nil {
				t.Fatal("uncaptured input was admitted")
			}
		})
	}
}

func TestNativeRecipeClearsAmbientSearchPaths(t *testing.T) {
	keys := []string{"CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH", "LIBRARY_PATH", "COMPILER_PATH", "GCC_EXEC_PREFIX"}
	for _, key := range keys {
		t.Setenv(key, "/uncaptured/input")
	}
	environment := (&builder{}).nativeEnvironment()
	for _, key := range keys {
		if value, present := environment[key]; !present || value != "" {
			t.Fatalf("ambient %s was not explicitly removed from the native recipe", key)
		}
	}
}
