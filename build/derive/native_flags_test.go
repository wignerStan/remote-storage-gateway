package main

import (
	"encoding/json"
	"os"
	"os/exec"
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
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b := &builder{}
	for _, explicit := range []bool{false, true} {
		name := "ambient"
		if explicit {
			name = "explicit-override"
		}
		t.Run(name, func(t *testing.T) {
			environment := b.nativeEnvironment()
			if explicit {
				for _, key := range keys {
					environment[key] = "/uncaptured/override"
				}
			}
			environment["GATEWAY_NATIVE_ENVIRONMENT_PROBE"] = "1"
			output, err := b.execute(t.TempDir(), executable, environment, "-test.run=^TestNativeRecipeEnvironmentProbe$")
			if err != nil {
				t.Fatal(err)
			}
			var present map[string]string
			if err = json.Unmarshal([]byte(output), &present); err != nil {
				t.Fatal(err)
			}
			if len(present) != 0 {
				t.Fatalf("compiler search variables survived in the actual child: %#v", present)
			}
		})
	}
}

func TestNativeRecipeEnvironmentProbe(t *testing.T) {
	if os.Getenv("GATEWAY_NATIVE_ENVIRONMENT_PROBE") != "1" {
		return
	}
	present := make(map[string]string)
	for _, key := range []string{"CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH", "LIBRARY_PATH", "COMPILER_PATH", "GCC_EXEC_PREFIX"} {
		if value, ok := os.LookupEnv(key); ok {
			present[key] = value
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(present); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestNativeRecipeRunsCompilerWithoutAmbientSearchPaths(t *testing.T) {
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("native C compiler is not installed")
	}
	for _, key := range []string{"CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH", "LIBRARY_PATH", "COMPILER_PATH", "GCC_EXEC_PREFIX"} {
		t.Setenv(key, "/uncaptured/input")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "probe.c")
	if err = os.WriteFile(source, []byte("int gateway_native_probe(void) { return 0; }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// An empty GCC_EXEC_PREFIX is not an unset variable: GCC then loses its
	// normal cc1 lookup. Exercise the real compiler through our process policy.
	if _, err = (&builder{}).execute(dir, compiler, (&builder{}).nativeEnvironment(), "-fsyntax-only", source); err != nil {
		t.Fatal(err)
	}
}
