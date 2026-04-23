package resolver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseBootstrapReadsManagementServerChannelCreds(t *testing.T) {
	dir := t.TempDir()
	bootstrapPath := filepath.Join(dir, "grpc-bootstrap.json")
	data := []byte(`{
  "xds_servers": [{
    "server_uri": "dubbod.dubbo-system.svc:15012",
    "channel_creds": [{
      "type": "tls",
      "config": {
        "certificate_file": "/etc/dubbo/proxy/cert-chain.pem",
        "private_key_file": "/etc/dubbo/proxy/key.pem",
        "ca_certificate_file": "/etc/dubbo/proxy/root-cert.pem"
      }
    }],
    "server_features": ["xds_v3"]
  }],
  "node": {"id": "proxyless~10.0.0.1~client.app~app.svc.cluster.local"},
  "certificate_providers": {
    "default": {
      "plugin_name": "file_watcher",
      "config": {
        "certificate_file": "/etc/dubbo/proxy/cert-chain.pem",
        "private_key_file": "/etc/dubbo/proxy/key.pem",
        "ca_certificate_file": "/etc/dubbo/proxy/root-cert.pem"
      }
    }
  }
}`)
	if err := os.WriteFile(bootstrapPath, data, 0o600); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}

	cfg, err := ParseBootstrap(bootstrapPath)
	if err != nil {
		t.Fatalf("ParseBootstrap() failed: %v", err)
	}
	if got, want := cfg.ServerURI, "dubbod.dubbo-system.svc:15012"; got != want {
		t.Fatalf("ServerURI = %q, want %q", got, want)
	}
	if got, want := len(cfg.ChannelCreds), 1; got != want {
		t.Fatalf("channel creds count = %d, want %d", got, want)
	}
	if got, want := cfg.ChannelCreds[0].Type, "tls"; got != want {
		t.Fatalf("channel creds type = %q, want %q", got, want)
	}
	if got, want := cfg.ChannelCreds[0].Config.CACertificateFile, "/etc/dubbo/proxy/root-cert.pem"; got != want {
		t.Fatalf("channel creds CA = %q, want %q", got, want)
	}
	if cfg.Node == nil || cfg.Node.Id != "proxyless~10.0.0.1~client.app~app.svc.cluster.local" {
		t.Fatalf("node id = %#v", cfg.Node)
	}
	if got, want := cfg.CertProviders["default"].CertificateFile, "/etc/dubbo/proxy/cert-chain.pem"; got != want {
		t.Fatalf("default cert provider certificate = %q, want %q", got, want)
	}
}

func TestTransportCredentialsFromBootstrap(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *BootstrapConfig
		protocol string
	}{
		{
			name: "unix sockets use insecure credentials",
			cfg: &BootstrapConfig{
				ServerURI: "unix:///tmp/xds.sock",
				ChannelCreds: []ChannelCredsConfig{{
					Type: "tls",
				}},
			},
			protocol: "insecure",
		},
		{
			name: "insecure channel credentials",
			cfg: &BootstrapConfig{
				ServerURI: "dubbod.dubbo-system.svc:15010",
				ChannelCreds: []ChannelCredsConfig{{
					Type: "insecure",
				}},
			},
			protocol: "insecure",
		},
		{
			name: "tls channel credentials",
			cfg: &BootstrapConfig{
				ServerURI: "dubbod.dubbo-system.svc:15012",
				ChannelCreds: []ChannelCredsConfig{{
					Type: "tls",
				}},
			},
			protocol: "tls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, err := TransportCredentialsFromBootstrap(tt.cfg)
			if err != nil {
				t.Fatalf("TransportCredentialsFromBootstrap() failed: %v", err)
			}
			if got := creds.Info().SecurityProtocol; got != tt.protocol {
				t.Fatalf("security protocol = %q, want %q", got, tt.protocol)
			}
		})
	}
}

func TestDialAddress(t *testing.T) {
	if got, want := DialAddress("unix:///tmp/xds.sock"), "unix:/tmp/xds.sock"; got != want {
		t.Fatalf("DialAddress() = %q, want %q", got, want)
	}
	if got, want := DialAddress("dubbod.dubbo-system.svc:15012"), "dubbod.dubbo-system.svc:15012"; got != want {
		t.Fatalf("DialAddress() = %q, want %q", got, want)
	}
}
