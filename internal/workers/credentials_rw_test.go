package workers

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritableCodexCredentialsMount(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.Mkdir(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatal(err)
	}

	dc := &DockerConfig{
		Image:         "worker",
		Lifecycle:     "disposable",
		CredentialsRW: []string{"codex"},
	}
	mounts, err := BuildContainerMounts(dc)
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 1 {
		t.Fatalf("mount count = %d, want 1", len(mounts))
	}
	if mounts[0].Target != "/home/worker/.codex" {
		t.Fatalf("target = %q", mounts[0].Target)
	}
	if mounts[0].ReadOnly {
		t.Fatal("writable Codex credential mount is read-only")
	}

	env, err := BuildContainerEnv(dc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if env["HOME"] != "/home/worker" {
		t.Fatalf("HOME = %q", env["HOME"])
	}
}

func TestWritableCredentialsValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  DockerConfig
	}{
		{
			name: "only codex allowed",
			cfg: DockerConfig{
				Image:         "worker",
				Lifecycle:     "disposable",
				CredentialsRW: []string{"claude"},
			},
		},
		{
			name: "disposable required",
			cfg: DockerConfig{
				Image:         "worker",
				Lifecycle:     "persistent",
				CredentialsRW: []string{"codex"},
			},
		},
		{
			name: "read only and writable conflict",
			cfg: DockerConfig{
				Image:         "worker",
				Lifecycle:     "disposable",
				Credentials:   []string{"codex"},
				CredentialsRW: []string{"codex"},
			},
		},
		{
			name: "token and writable conflict",
			cfg: DockerConfig{
				Image:         "worker",
				Lifecycle:     "disposable",
				CredentialsRW: []string{"codex"},
				Token:         "codex.default",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestWritableCredentialsDeepCopy(t *testing.T) {
	entry := &Entry{Worker: &Worker{
		Name: "worker",
		Docker: &DockerConfig{
			Image:         "worker",
			Lifecycle:     "disposable",
			CredentialsRW: []string{"codex"},
		},
	}}
	copied := deepCopyEntry(entry)
	copied.Worker.Docker.CredentialsRW[0] = "changed"
	if entry.Worker.Docker.CredentialsRW[0] != "codex" {
		t.Fatal("deep copy aliases writable credentials")
	}
}
