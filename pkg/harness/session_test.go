package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/mainplane-ai/mainplane/pkg/provider"
	"github.com/mainplane-ai/mainplane/pkg/statefile"
	"github.com/mainplane-ai/mainplane/pkg/version"
	"github.com/mainplane-ai/mainplane/pkg/worker"
)

func TestSessionStaysSteppingUntilLogCopyFinishes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, peer := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = peer.Close() }()
	writes := make(chan worker.Frame, 1)
	release := make(chan struct{})
	go func() {
		_ = worker.Encode(peer, worker.Frame{Header: worker.Header{
			Kind: worker.Hello, Version: version.V, Scratch: "/fixture",
		}})
		frame, err := worker.Decode(bufio.NewReader(peer))
		if err != nil {
			return
		}
		writes <- frame
		<-release
		_ = worker.Encode(peer, worker.Frame{Header: worker.Header{Kind: worker.Done, ID: frame.ID}})
	}()

	remote, err := Connect(client, func() (string, error) { return "fixture", nil })
	if err != nil {
		t.Fatal(err)
	}
	pool := NewPool(nil)
	pool.Add(remote)
	p := provider.Anthropic("", "")
	p.Run = func(_ context.Context, _ []byte, emit func(statefile.Record)) (statefile.Header, error) {
		emit(statefile.Record{Header: statefile.Header{Kind: statefile.Text, Type: "text/plain"}, Body: []byte("done")})
		return statefile.Header{Stop: "end_turn", Usage: &statefile.Usage{Input: 10, Output: 1}}, nil
	}
	h := &Harness{
		Sessions:  statefile.Sessions{Dir: t.TempDir()},
		Workers:   pool,
		Providers: map[string]provider.Provider{"fixture": p},
	}
	conf, err := json.Marshal(statefile.Conf{
		Model: "fixture/model", Tools: "default", ContextLimit: 1_000,
		Workers: []statefile.Worker{{Name: "fixture"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := statefile.NewID()
	f, err := h.Sessions.Start(id, conf)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Post(ctx, id, "test", "", []Part{{Type: "text/plain", Body: []byte("finish")}}); err != nil {
		t.Fatal(err)
	}

	select {
	case frame := <-writes:
		if frame.Kind != worker.Write || frame.Path != "/fixture/logs/"+id+".log" {
			t.Fatalf("unexpected log write: kind=%s path=%s", frame.Kind, frame.Path)
		}
	case <-ctx.Done():
		t.Fatal("log write did not begin")
	}
	info, err := h.Info(id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != statefile.StatusStepping {
		t.Fatalf("status during log export = %s, want %s", info.Status, statefile.StatusStepping)
	}

	close(release)
	s := h.session(id)
	h.mu.Lock()
	done := s.done
	h.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("session did not finish after log acknowledgment")
	}
	info, err = h.Info(id)
	if err != nil {
		t.Fatal(err)
	}
	if info.Status != statefile.StatusClosed {
		t.Fatalf("status after log export = %s, want %s", info.Status, statefile.StatusClosed)
	}
}
