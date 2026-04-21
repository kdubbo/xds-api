package server

import "testing"

func TestNormalizeDialTargetFromEnv(t *testing.T) {
	t.Setenv(envXDSResolver, xdsSchemePrefix)

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "plain service target",
			target: "svc.ns.svc.cluster.local:7070",
			want:   "xds:///svc.ns.svc.cluster.local:7070",
		},
		{
			name:   "dns target",
			target: "dns:///svc.ns.svc.cluster.local:7070",
			want:   "xds:///svc.ns.svc.cluster.local:7070",
		},
		{
			name:   "passthrough target",
			target: "passthrough:///svc.ns.svc.cluster.local:7070",
			want:   "xds:///svc.ns.svc.cluster.local:7070",
		},
		{
			name:   "existing xds target",
			target: "xds:///svc.ns.svc.cluster.local:7070",
			want:   "xds:///svc.ns.svc.cluster.local:7070",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, useXDS := normalizeDialTarget(tt.target)
			if !useXDS {
				t.Fatalf("useXDS = false, want true")
			}
			if got != tt.want {
				t.Fatalf("target = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeDialTargetRequiresOptIn(t *testing.T) {
	got, useXDS := normalizeDialTarget("svc.ns.svc.cluster.local:7070")
	if useXDS {
		t.Fatalf("useXDS = true, want false")
	}
	if got != "svc.ns.svc.cluster.local:7070" {
		t.Fatalf("target = %q, want original target", got)
	}
}

func TestNormalizeDialTargetRejectsUnsupportedScheme(t *testing.T) {
	t.Setenv(envXDSResolver, xdsSchemePrefix)

	got, useXDS := normalizeDialTarget("unix:///tmp/grpc.sock")
	if useXDS {
		t.Fatalf("useXDS = true, want false")
	}
	if got != "unix:///tmp/grpc.sock" {
		t.Fatalf("target = %q, want original target", got)
	}
}

func TestXDSCredentialsEnv(t *testing.T) {
	if !xdsCredentialsEnabled() {
		t.Fatalf("xdsCredentialsEnabled() = false, want true by default")
	}

	t.Setenv(envXDSCredentials, "false")
	if xdsCredentialsEnabled() {
		t.Fatalf("xdsCredentialsEnabled() = true, want false")
	}
}
