package solace

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadVarsFile(t *testing.T) {
	t.Run("missing file is not an error", func(t *testing.T) {
		v, err := LoadVarsFile(filepath.Join(t.TempDir(), "does-not-exist.vars"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Errorf("expected nil map, got %v", v)
		}
	})

	t.Run("parses valid file", func(t *testing.T) {
		body := `# top-level comment
namespace = dev-payments-1
vpn=payments-dev

# blank line above
creds=/home/user/secrets
token=k=v=both
`
		path := filepath.Join(t.TempDir(), "test.vars")
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		got, err := LoadVarsFile(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := map[string]string{
			"namespace": "dev-payments-1",
			"vpn":       "payments-dev",
			"creds":     "/home/user/secrets",
			"token":     "k=v=both",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("missing equals errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.vars")
		if err := os.WriteFile(path, []byte("namespace dev\n"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := LoadVarsFile(path); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("empty key errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.vars")
		if err := os.WriteFile(path, []byte("=value\n"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := LoadVarsFile(path); err == nil {
			t.Error("expected error, got nil")
		}
	})
}

func TestDefaultVarsPath(t *testing.T) {
	cases := []struct {
		config string
		want   string
	}{
		{"/etc/solaz/dev.conf", "/etc/solaz/dev.vars"},
		{"/etc/solaz/dev.json", "/etc/solaz/dev.vars"},
		{"/etc/solaz/dev", "/etc/solaz/dev.vars"},
		{".solaz.conf", ".solaz.vars"},
	}
	for _, tc := range cases {
		if got := VarsPath(tc.config); got != tc.want {
			t.Errorf("DefaultVarsPath(%q) = %q, want %q", tc.config, got, tc.want)
		}
	}
}

func TestTypesPath(t *testing.T) {
	cases := []struct {
		config string
		want   string
	}{
		{"/etc/solaz/dev.conf", "/etc/solaz/dev.types"},
		{"/etc/solaz/dev.json", "/etc/solaz/dev.types"},
		{"/etc/solaz/dev", "/etc/solaz/dev.types"},
		{".solaz.conf", ".solaz.types"},
	}
	for _, tc := range cases {
		if got := TypesPath(tc.config); got != tc.want {
			t.Errorf("TypesPath(%q) = %q, want %q", tc.config, got, tc.want)
		}
	}
}

func TestWriteTypesFile(t *testing.T) {
	t.Run("creates file when missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "new.types")
		updates := map[string]string{
			"trades/orders/btc": "com.example.Order",
			"trades/fills/btc":  "com.example.Fill",
		}
		if err := WriteTypesFile(path, updates); err != nil {
			t.Fatalf("WriteTypesFile: %v", err)
		}
		got, err := LoadTypesFile(path)
		if err != nil {
			t.Fatalf("LoadTypesFile: %v", err)
		}
		if !reflect.DeepEqual(got, updates) {
			t.Errorf("round-trip mismatch: got %v, want %v", got, updates)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if !strings.HasPrefix(string(data), "# auto-generated") {
			t.Errorf("expected auto-generated header comment, got:\n%s", data)
		}
	})

	t.Run("merges with existing entries", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "merge.types")
		initial := "# user note\nkeep/a=Old\nreplace/b=Old\n"
		if err := os.WriteFile(path, []byte(initial), 0644); err != nil {
			t.Fatalf("write seed: %v", err)
		}
		updates := map[string]string{
			"replace/b": "New",
			"new/c":     "Fresh",
		}
		if err := WriteTypesFile(path, updates); err != nil {
			t.Fatalf("WriteTypesFile: %v", err)
		}
		got, err := LoadTypesFile(path)
		if err != nil {
			t.Fatalf("LoadTypesFile: %v", err)
		}
		want := map[string]string{
			"keep/a":    "Old",
			"replace/b": "New",
			"new/c":     "Fresh",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("merged content mismatch: got %v, want %v", got, want)
		}
	})

	t.Run("write is atomic", func(t *testing.T) {
		// Verify no stray .tmp.* siblings are left behind after a successful write.
		dir := t.TempDir()
		path := filepath.Join(dir, "atomic.types")
		if err := WriteTypesFile(path, map[string]string{"x": "Y"}); err != nil {
			t.Fatalf("WriteTypesFile: %v", err)
		}
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(ents) != 1 || ents[0].Name() != "atomic.types" {
			names := make([]string, len(ents))
			for i, e := range ents {
				names[i] = e.Name()
			}
			t.Errorf("expected only atomic.types in dir, got %v", names)
		}
	})
}
