package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// boundedWire is a wire whose every hop has a deadline of its own.
type boundedWire struct{ d time.Duration }

func (boundedWire) authLabel() string                                    { return "none" }
func (boundedWire) name() string                                         { return "bounded" }
func (boundedWire) owns(*http.Request) bool                              { return true }
func (boundedWire) prepare(*reqInfo, []byte, string) string              { return "" }
func (boundedWire) apply(*http.Request, reqInfo)                         {}
func (boundedWire) translate(r *http.Response, _ reqInfo) *http.Response { return r }
func (boundedWire) reauth(*http.Request) bool                            { return false }
func (w boundedWire) hopDeadline(reqInfo) time.Duration                  { return w.d }

type hopRoundTrip func(*http.Request) (*http.Response, error)

func (f hopRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A hop's own deadline ends a hop that hangs, while its request lives on,
// and lasts until the reply's body is closed.
func TestHopDeadlineBoundsTheHopNotTheRequest(t *testing.T) {
	var hopCtx context.Context
	tr := &failoverTransport{base: hopRoundTrip(func(r *http.Request) (*http.Response, error) {
		hopCtx = r.Context()
		if r.URL.Path == "/hang" {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})}
	ctx := context.WithValue(context.Background(), infoKey, reqInfo{wire: boundedWire{50 * time.Millisecond}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://upstream.invalid/hang", nil)
	start := time.Now()
	if _, err := tr.tripWithReauth(req); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err %v after %s, want the hop's own deadline", err, time.Since(start))
	}
	if ctx.Err() != nil {
		t.Fatal("the request's context ended with the hop's")
	}
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, "http://upstream.invalid/ok", nil)
	resp, err := tr.tripWithReauth(req)
	if err != nil {
		t.Fatal(err)
	}
	if hopCtx.Err() != nil {
		t.Fatal("the hop's deadline ended before its body was read")
	}
	_ = resp.Body.Close()
	if hopCtx.Err() == nil {
		t.Fatal("the hop's deadline outlived its body")
	}
}
