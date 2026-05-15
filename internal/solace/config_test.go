package solace_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dnswlt/hackz/solaz/internal/solace"
)

func TestLoadVarsFile(t *testing.T) {
	t.Run("missing file is not an error", func(t *testing.T) {
		v, err := solace.LoadVarsFile(filepath.Join(t.TempDir(), "does-not-exist.vars"))
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
		got, err := solace.LoadVarsFile(path)
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
		if _, err := solace.LoadVarsFile(path); err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("empty key errors", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "bad.vars")
		if err := os.WriteFile(path, []byte("=value\n"), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := solace.LoadVarsFile(path); err == nil {
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
		if got := solace.VarsPath(tc.config); got != tc.want {
			t.Errorf("DefaultVarsPath(%q) = %q, want %q", tc.config, got, tc.want)
		}
	}
}
