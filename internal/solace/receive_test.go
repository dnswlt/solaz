package solace

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
)

// fakeReceiver returns msg (if set) after delay, or err (if set);
// otherwise it waits out the requested timeout and reports a
// TimeoutError, like the SDK does.
type fakeReceiver struct {
	msg   message.InboundMessage
	delay time.Duration
	err   error
	calls int
}

func (r *fakeReceiver) ReceiveMessage(timeout time.Duration) (message.InboundMessage, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	if r.msg != nil && r.delay <= timeout {
		time.Sleep(r.delay)
		return r.msg, nil
	}
	time.Sleep(timeout)
	if r.msg != nil {
		r.delay -= timeout
	}
	return nil, solace.NewError(&solace.TimeoutError{}, "timed out", nil)
}

func TestReceiveNext(t *testing.T) {
	msg := fakeMessage{}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name        string
		ctx         context.Context
		r           *fakeReceiver
		idleTimeout time.Duration
		deadline    time.Duration // relative to now; 0 means none
		wantMsg     bool
		wantErr     string // "" means no error
		wantNoCalls bool
	}{
		{name: "message available", r: &fakeReceiver{msg: msg}, wantMsg: true},
		{name: "message after several polls", r: &fakeReceiver{msg: msg, delay: 450 * time.Millisecond}, wantMsg: true},
		{name: "context already canceled", ctx: canceled, r: &fakeReceiver{msg: msg}, wantNoCalls: true},
		{name: "deadline passes", r: &fakeReceiver{}, deadline: 50 * time.Millisecond},
		{name: "idle timeout", r: &fakeReceiver{}, idleTimeout: 50 * time.Millisecond, wantErr: "no message within 50ms"},
		{name: "deadline before idle timeout", r: &fakeReceiver{}, idleTimeout: time.Second, deadline: 50 * time.Millisecond},
		{name: "receiver error", r: &fakeReceiver{err: errors.New("boom")}, wantErr: "boom"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := c.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			var deadline time.Time
			if c.deadline > 0 {
				deadline = time.Now().Add(c.deadline)
			}
			got, err := receiveNext(ctx, c.r, c.idleTimeout, deadline)
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Fatalf("error = %v, want substring %q", err, c.wantErr)
			}
			if (got != nil) != c.wantMsg {
				t.Errorf("message = %v, wantMsg %v", got, c.wantMsg)
			}
			if c.wantNoCalls && c.r.calls != 0 {
				t.Errorf("ReceiveMessage called %d times, want 0", c.r.calls)
			}
		})
	}
}

// TestReceiveNext_CancelWhileWaiting checks that a cancellation during an
// indefinite wait (no idle timeout, no deadline) is noticed within about
// one poll interval.
func TestReceiveNext_CancelWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()
	got, err := receiveNext(ctx, &fakeReceiver{}, 0, time.Time{})
	if err != nil || got != nil {
		t.Fatalf("receiveNext = %v, %v; want nil, nil", got, err)
	}
	if elapsed := time.Since(start); elapsed > 2*receivePollInterval {
		t.Errorf("returned after %s, want <= %s", elapsed, 2*receivePollInterval)
	}
}
