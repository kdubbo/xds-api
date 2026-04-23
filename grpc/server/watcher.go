package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	tlsv1 "github.com/kdubbo/xds-api/extensions/transport_sockets/tls/v1"
	xdsresolver "github.com/kdubbo/xds-api/grpc/resolver"
	listenerv1 "github.com/kdubbo/xds-api/listener/v1"
	discovery "github.com/kdubbo/xds-api/service/discovery/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	listenerType = "type.googleapis.com/listener.v1.Listener"
	// serverListenerNameTemplate matches the bootstrap server_listener_resource_name_template.
	serverListenerNameTemplate = "xds.dubbo.apache.org/grpc/lds/inbound/%s"
)

// TLSMode represents the server-side TLS operating mode derived from xDS.
type TLSMode int

const (
	// TLSModeUnknown means the inbound listener has not been received yet.
	TLSModeUnknown TLSMode = iota
	// TLSModePlaintext means no DownstreamTlsContext on the inbound filter chain.
	TLSModePlaintext
	// TLSModeMTLS means a DownstreamTlsContext with client cert requirement was found.
	TLSModeMTLS
)

// InboundTLSConfig carries the resolved TLS decision for a server listener.
type InboundTLSConfig struct {
	Mode       TLSMode
	Downstream *tlsv1.DownstreamTlsContext // non-nil only when Mode == TLSModeMTLS
}

// Watcher subscribes to the xDS inbound listener for a given address and
// delivers TLS configuration updates via a channel.
type Watcher struct {
	addr      string // e.g. "0.0.0.0:17070"
	bootstrap string // path to grpc-bootstrap.json

	mu       sync.Mutex
	current  *InboundTLSConfig
	updateCh chan *InboundTLSConfig
	closeCh  chan struct{}
}

// NewWatcher creates a Watcher for the inbound listener of the given address.
// bootstrapPath defaults to the GRPC_XDS_BOOTSTRAP env var if empty.
func NewWatcher(addr, bootstrapPath string) *Watcher {
	if bootstrapPath == "" {
		bootstrapPath = os.Getenv("GRPC_XDS_BOOTSTRAP")
	}
	return &Watcher{
		addr:      addr,
		bootstrap: bootstrapPath,
		updateCh:  make(chan *InboundTLSConfig, 8),
		closeCh:   make(chan struct{}),
	}
}

// UpdateCh returns the channel on which TLS config updates are delivered.
func (w *Watcher) UpdateCh() <-chan *InboundTLSConfig {
	return w.updateCh
}

// Start begins watching in a background goroutine.
func (w *Watcher) Start() {
	go w.run()
}

// Close stops the watcher.
func (w *Watcher) Close() {
	close(w.closeCh)
}

// WaitForInitial blocks until the first xDS response is received or ctx is done.
// Returns the initial TLS config.
func (w *Watcher) WaitForInitial(ctx context.Context) (*InboundTLSConfig, error) {
	select {
	case cfg := <-w.updateCh:
		return cfg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *Watcher) run() {
	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	for {
		select {
		case <-w.closeCh:
			return
		default:
		}
		w.connect()
		// connect() returns only on error or close. Apply backoff before
		// reconnecting so we don't hammer the control plane on transient failures.
		select {
		case <-w.closeCh:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (w *Watcher) connect() {
	bootstrap, err := xdsresolver.ParseBootstrap(w.bootstrap)
	if err != nil {
		log.Printf("[xds-server-watcher] failed to parse bootstrap: %v", err)
		return
	}

	addr := xdsresolver.DialAddress(bootstrap.ServerURI)
	dialOptions, err := xdsresolver.DialOptionsFromBootstrap(bootstrap)
	if err != nil {
		log.Printf("[xds-server-watcher] failed to build dial options from bootstrap: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-w.closeCh
		cancel()
	}()
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, dialOptions...)
	if err != nil {
		log.Printf("[xds-server-watcher] failed to dial %s: %v", addr, err)
		return
	}
	defer conn.Close()

	svcClient := discovery.NewAggregatedDiscoveryServiceClient(conn)
	stream, err := svcClient.StreamAggregatedResources(ctx)
	if err != nil {
		log.Printf("[xds-server-watcher] failed to open ADS stream: %v", err)
		return
	}

	listenerName := fmt.Sprintf(serverListenerNameTemplate, w.addr)
	// Subscribe with both the precise listener name AND wildcard so we receive
	// full pushes triggered by PeerAuthentication/DestinationRule changes as
	// well as targeted pushes for our specific inbound listener.
	if err := stream.Send(&discovery.DiscoveryRequest{
		Node:          bootstrap.Node,
		TypeUrl:       listenerType,
		ResourceNames: []string{listenerName},
	}); err != nil {
		log.Printf("[xds-server-watcher] failed to subscribe to inbound listener: %v", err)
		return
	}
	log.Printf("[xds-server-watcher] subscribed to inbound listener: %s", listenerName)

	for {
		select {
		case <-w.closeCh:
			return
		default:
		}

		resp, err := stream.Recv()
		if err != nil {
			log.Printf("[xds-server-watcher] recv error: %v", err)
			return
		}

		// ACK while keeping the precise listener name subscription alive.
		_ = stream.Send(&discovery.DiscoveryRequest{
			Node:          bootstrap.Node,
			TypeUrl:       resp.TypeUrl,
			VersionInfo:   resp.VersionInfo,
			ResponseNonce: resp.Nonce,
			ResourceNames: []string{listenerName},
		})

		if resp.TypeUrl != listenerType {
			continue
		}

		cfg := w.parseTLSFromLDS(resp, listenerName)
		if cfg == nil {
			// Listener was not present in this response — this is a full push
			// that doesn't include our inbound listener (e.g. a push triggered
			// by another resource type).  Do NOT update the channel: keeping
			// the current mode avoids erroneously flipping back to plaintext
			// during a PeerAuthentication-triggered full push.
			continue
		}
		w.mu.Lock()
		w.current = cfg
		w.mu.Unlock()

		select {
		case w.updateCh <- cfg:
		default:
			// Drop stale update if consumer is slow; keep latest.
			select {
			case <-w.updateCh:
			default:
			}
			w.updateCh <- cfg
		}
	}
}

// parseTLSFromLDS scans the LDS response for the named listener and returns
// its TLS configuration.  Returns nil if the listener is not present in the
// response (caller should retain the current mode rather than defaulting to
// plaintext).
func (w *Watcher) parseTLSFromLDS(resp *discovery.DiscoveryResponse, listenerName string) *InboundTLSConfig {
	for _, resource := range resp.Resources {
		lis := &listenerv1.Listener{}
		if err := proto.Unmarshal(resource.Value, lis); err != nil {
			log.Printf("[xds-server-watcher] failed to unmarshal Listener: %v", err)
			continue
		}
		if lis.Name != listenerName {
			continue
		}
		for _, fc := range lis.GetFilterChains() {
			ts := fc.GetTransportSocket()
			if ts == nil {
				continue
			}
			typedCfg := ts.GetTypedConfig()
			if typedCfg == nil {
				continue
			}
			downstream := &tlsv1.DownstreamTlsContext{}
			if err := anypb.UnmarshalTo(typedCfg, downstream, proto.UnmarshalOptions{}); err != nil {
				continue
			}
			log.Printf("[xds-server-watcher] listener %s has DownstreamTlsContext (mTLS)", listenerName)
			return &InboundTLSConfig{Mode: TLSModeMTLS, Downstream: downstream}
		}
		log.Printf("[xds-server-watcher] listener %s has no DownstreamTlsContext (plaintext)", listenerName)
		return &InboundTLSConfig{Mode: TLSModePlaintext}
	}
	// Listener not found in this response — return nil to signal "no change".
	log.Printf("[xds-server-watcher] listener %s not found in LDS response (%d resources), retaining current mode",
		listenerName, len(resp.Resources))
	return nil
}
