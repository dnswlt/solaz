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

func TestResolveAuthScheme(t *testing.T) {
	cases := []struct {
		name    string
		profile Profile
		want    string
		wantErr bool
	}{
		{
			name:    "explicit client_cert",
			profile: Profile{AuthScheme: AuthSchemeClientCert},
			want:    AuthSchemeClientCert,
		},
		{
			name:    "explicit basic",
			profile: Profile{AuthScheme: AuthSchemeBasic},
			want:    AuthSchemeBasic,
		},
		{
			name:    "explicit unknown errors",
			profile: Profile{AuthScheme: "kerberos"},
			wantErr: true,
		},
		{
			name:    "derived from cert file",
			profile: Profile{ClientCertFile: "/etc/c.pem"},
			want:    AuthSchemeClientCert,
		},
		{
			name:    "derived from cert key alone (cert file missing) — ValidateProfile catches the gap",
			profile: Profile{ClientKeyFile: "/etc/k.pem"},
			want:    AuthSchemeClientCert,
		},
		{
			name:    "derived from cert username alone",
			profile: Profile{ClientCertUserName: "svc"},
			want:    AuthSchemeClientCert,
		},
		{
			name:    "derived from basic username",
			profile: Profile{BasicUsername: "u"},
			want:    AuthSchemeBasic,
		},
		{
			name:    "derived from basic password alone — ValidateProfile catches the gap",
			profile: Profile{BasicPassword: "p"},
			want:    AuthSchemeBasic,
		},
		{
			name:    "ambiguous: both cert and basic fields set",
			profile: Profile{ClientCertFile: "/etc/c.pem", BasicUsername: "u"},
			wantErr: true,
		},
		{
			name:    "no auth fields populated",
			profile: Profile{},
			wantErr: true,
		},
		{
			name:    "explicit overrides ambiguity",
			profile: Profile{AuthScheme: AuthSchemeBasic, ClientCertFile: "/etc/c.pem", BasicUsername: "u"},
			want:    AuthSchemeBasic,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAuthScheme(&tc.profile)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got scheme %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("scheme = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidateProfile(t *testing.T) {
	base := func() Profile {
		return Profile{Name: "p", Host: "tcps://broker:55443", VPN: "v"}
	}
	cases := []struct {
		name    string
		mutate  func(p *Profile)
		wantErr bool
	}{
		{
			name: "client_cert: cert + key satisfy",
			mutate: func(p *Profile) {
				p.ClientCertFile = "/etc/c.pem"
				p.ClientKeyFile = "/etc/k.pem"
			},
		},
		{
			name: "client_cert: missing key errors",
			mutate: func(p *Profile) {
				p.ClientCertFile = "/etc/c.pem"
			},
			wantErr: true,
		},
		{
			name: "basic: username + password satisfy",
			mutate: func(p *Profile) {
				p.BasicUsername = "u"
				p.BasicPassword = "p"
			},
		},
		{
			name: "basic: missing password errors",
			mutate: func(p *Profile) {
				p.BasicUsername = "u"
			},
			wantErr: true,
		},
		{
			name: "explicit basic respects fields even with cert fields present",
			mutate: func(p *Profile) {
				p.AuthScheme = AuthSchemeBasic
				p.BasicUsername = "u"
				p.BasicPassword = "p"
				p.ClientCertFile = "/etc/c.pem" // ignored when scheme is explicit
			},
		},
		{
			name:    "host missing",
			mutate:  func(p *Profile) { p.Host = "" },
			wantErr: true,
		},
		{
			name:    "vpn missing",
			mutate:  func(p *Profile) { p.VPN = "" },
			wantErr: true,
		},
		{
			name:    "no auth fields at all errors",
			mutate:  func(p *Profile) {},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			tc.mutate(&p)
			err := ValidateProfile(&p, "test.conf")
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
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

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		wantErr string // "" means success; otherwise a substring of the error
	}{
		{
			name: "profile referencing a proto set",
			config: `{
				"proto_sets": {"payments": {"paths": ["/p"]}},
				"profiles": [{"name": "qa", "proto_set": "payments"}]
			}`,
		},
		{
			name:   "profile without proto set",
			config: `{"profiles": [{"name": "qa"}]}`,
		},
		{
			name: "unknown proto set",
			config: `{
				"proto_sets": {"payments": {"paths": ["/p"]}},
				"profiles": [{"name": "qa", "proto_set": "billing"}]
			}`,
			wantErr: `unknown proto_set "billing" (available: payments)`,
		},
		{
			name: "proto set without paths",
			config: `{
				"proto_sets": {"payments": {"topic_types": {"a/>": "A"}}},
				"profiles": [{"name": "qa"}]
			}`,
			wantErr: `proto set "payments" has no paths`,
		},
		{
			name:    "inline proto_paths is rejected",
			config:  `{"profiles": [{"name": "qa", "proto_paths": ["/p"]}]}`,
			wantErr: `unknown field "proto_paths"`,
		},
		{
			name:    "unknown top-level key",
			config:  `{"profiles": [{"name": "qa"}], "profles": []}`,
			wantErr: `unknown field "profles"`,
		},
		{
			name:    "trailing data",
			config:  `{"profiles": [{"name": "qa"}]} }`,
			wantErr: "unexpected data after top-level object",
		},
		{
			name:    "no profiles",
			config:  `{"proto_sets": {}}`,
			wantErr: "no profiles defined",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "solaz.conf")
			if err := os.WriteFile(path, []byte(tc.config), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestSelectProfile_ResolvesProtoSet(t *testing.T) {
	c := &Config{
		ProtoSets: map[string]ProtoSet{
			"payments": {Paths: []string{"/p"}, TopicTypes: map[string]string{"a/>": "A"}},
		},
		Profiles: []Profile{
			{Name: "qa", ProtoSet: "payments"},
			{Name: "bare"},
		},
	}

	p, err := SelectProfile(c, "qa", "solaz.conf")
	if err != nil {
		t.Fatalf("SelectProfile: %v", err)
	}
	if want := []string{"/p"}; !reflect.DeepEqual(p.ProtoPaths, want) {
		t.Errorf("ProtoPaths = %v, want %v", p.ProtoPaths, want)
	}
	if want := map[string]string{"a/>": "A"}; !reflect.DeepEqual(p.TopicTypes, want) {
		t.Errorf("TopicTypes = %v, want %v", p.TopicTypes, want)
	}

	// The returned profile is a copy: mutating it must not leak into the
	// shared proto set or the config's profile list.
	p.ProtoPaths[0] = "/changed"
	p.TopicTypes["b/>"] = "B"
	p.Host = "changed"
	if s := c.ProtoSets["payments"]; s.Paths[0] != "/p" || len(s.TopicTypes) != 1 {
		t.Errorf("proto set mutated through profile: %+v", s)
	}
	if c.Profiles[0].Host != "" {
		t.Errorf("config profile mutated through returned copy")
	}

	bare, err := SelectProfile(c, "bare", "solaz.conf")
	if err != nil {
		t.Fatalf("SelectProfile: %v", err)
	}
	if bare.ProtoPaths != nil || bare.TopicTypes != nil {
		t.Errorf("profile without proto_set got ProtoPaths=%v TopicTypes=%v", bare.ProtoPaths, bare.TopicTypes)
	}

	if first, err := SelectProfile(c, "", "solaz.conf"); err != nil || first.Name != "qa" {
		t.Errorf("SelectProfile(\"\") = %v, %v; want first profile", first, err)
	}
	if _, err := SelectProfile(c, "nope", "solaz.conf"); err == nil {
		t.Errorf("SelectProfile(\"nope\"): expected error")
	}
}
