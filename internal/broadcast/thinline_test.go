package broadcast

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MattCheramie/GopherTrunk/internal/trunking"
)

// fakeThinline is a minimal Thinline Radio server: it welcomes a site
// that says hello with the right key, records every frame, asks for an
// upload whenever a call is ready (unless told to discard), and accepts
// call uploads.
type fakeThinline struct {
	t       *testing.T
	discard bool
	status  int // upload response status; 0 = 200 OK

	mu       sync.Mutex
	frames   []map[string]any
	uploads  []map[string]string
	attempts int
}

func (f *fakeThinline) handler() http.Handler {
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/sdrtrunk", func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		var wmu sync.Mutex
		write := func(v any) {
			wmu.Lock()
			defer wmu.Unlock()
			_ = c.WriteJSON(v)
		}
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var m map[string]any
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			f.mu.Lock()
			f.frames = append(f.frames, m)
			f.mu.Unlock()
			switch m["type"] {
			case "hello":
				if m["key"] != "SITEKEY" {
					write(map[string]any{"type": "error", "error": "not a site control key"})
					return
				}
				write(map[string]any{"type": "welcome"})
			case "call.ready":
				cmd := "uploadCall"
				if f.discard {
					cmd = "discardCall"
				}
				write(map[string]any{"type": "command", "command": cmd, "recordingId": m["recordingId"], "id": "c1"})
			}
		}
	})
	mux.HandleFunc("/api/call-upload", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			f.t.Errorf("parse multipart: %v", err)
			return
		}
		got := map[string]string{}
		for k, v := range r.MultipartForm.Value {
			got[k] = v[0]
		}
		if _, _, err := r.FormFile("audio"); err != nil {
			f.t.Errorf("audio part missing: %v", err)
		}
		f.mu.Lock()
		f.attempts++
		status := f.status
		if status == 0 {
			f.uploads = append(f.uploads, got)
		}
		f.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("nope"))
			return
		}
		_, _ = w.Write([]byte("Call imported successfully."))
	})
	return mux
}

func (f *fakeThinline) framesOf(typ string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, m := range f.frames {
		if m["type"] == typ {
			out = append(out, m)
		}
	}
	return out
}

func (f *fakeThinline) uploadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

func newThinlineFor(t *testing.T, url string) *thinlineCore {
	t.Helper()
	b, err := NewThinlineCore(ThinlineCoreConfig{URL: url + "/api/call-upload/", APIKey: "SITEKEY", SystemID: 7})
	if err != nil {
		t.Fatal(err)
	}
	return b.(*thinlineCore)
}

func TestThinlineCoreStartedReadyUploadOnRequest(t *testing.T) {
	f := &fakeThinline{t: t}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	b := newThinlineFor(t, srv.URL)
	defer b.Close()

	c := testCall(t)
	c.DeviceSerial = "rtl0"
	c.CallStartedAt = c.StartedAt
	c.PatchedGroups = []uint32{11, 12}

	// The first start opens the websocket; the welcome then announces the
	// in-flight call.
	b.CallStarted(trunking.CallStart{
		Grant:        trunking.Grant{System: "Metro", GroupID: 4321, SourceID: 9001, FrequencyHz: 851_012_500},
		DeviceSerial: "rtl0",
		StartedAt:    c.StartedAt,
	})
	waitFor(t, func() bool { return len(f.framesOf("call.started")) == 1 })
	if len(f.framesOf("core.hello")) != 1 {
		t.Fatalf("core.hello not sent after welcome")
	}
	startedID := f.framesOf("call.started")[0]["recordingId"]

	if err := b.Send(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.uploadCount() == 1 })

	ready := f.framesOf("call.ready")
	if len(ready) != 1 || ready[0]["recordingId"] != startedID {
		t.Fatalf("call.ready must reuse the started recording ID %v; got %+v", startedID, ready)
	}
	up := f.uploads[0]
	for k, want := range map[string]string{
		"key": "SITEKEY", "system": "7", "talkgroup": "4321", "source": "9001",
		"frequency": "851012500", "patches": "[4321,11,12]", "recordingId": startedID.(string),
	} {
		if up[k] != want {
			t.Errorf("upload %s = %q, want %q", k, up[k], want)
		}
	}
	if up["duration"] == "" || up["dateTime"] == "" {
		t.Errorf("upload missing duration/dateTime: %+v", up)
	}
	waitFor(t, func() bool { return b.HeldCount() == 0 })
	if res := f.framesOf("result"); len(res) != 1 || res[0]["ok"] != true {
		t.Fatalf("result frame = %+v", res)
	}
}

func TestThinlineCoreDiscard(t *testing.T) {
	f := &fakeThinline{t: t, discard: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	b := newThinlineFor(t, srv.URL)

	b.ensureConnected()
	waitFor(t, func() bool { return len(f.framesOf("core.hello")) == 1 })
	if err := b.Send(context.Background(), testCall(t)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return b.HeldCount() == 0 })
	b.Close()
	if f.uploadCount() != 0 {
		t.Fatalf("a discarded call must not be uploaded, even on shutdown")
	}
}

func TestThinlineCoreUploadsDirectlyWhenServerUnreachable(t *testing.T) {
	f := &fakeThinline{t: t}
	mux := http.NewServeMux()
	// No websocket endpoint: the dial fails, so held calls go straight up.
	mux.Handle("/api/call-upload", f.handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()
	b := newThinlineFor(t, srv.URL)
	defer b.Close()

	if err := b.Send(context.Background(), testCall(t)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return f.uploadCount() == 1 })
}

func TestThinlineCoreReportsUploadFailure(t *testing.T) {
	f := &fakeThinline{t: t, status: http.StatusServiceUnavailable}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	b := newThinlineFor(t, srv.URL)
	defer b.Close()

	b.ensureConnected()
	waitFor(t, func() bool { return len(f.framesOf("core.hello")) == 1 })
	if err := b.Send(context.Background(), testCall(t)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(f.framesOf("call.uploadFailed")) == 1 })
	f.mu.Lock()
	attempts := f.attempts
	f.mu.Unlock()
	if attempts != thinlineUploadAttempts {
		t.Fatalf("a 5xx upload is retried once on a new connection; attempts = %d", attempts)
	}
	if b.HeldCount() != 1 {
		t.Fatalf("a failed upload stays held for the server to ask again")
	}
}

func TestThinlineCoreCancelsStartedCallOnClose(t *testing.T) {
	f := &fakeThinline{t: t}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	b := newThinlineFor(t, srv.URL)

	b.CallStarted(trunking.CallStart{Grant: trunking.Grant{GroupID: 5}, DeviceSerial: "rtl0", StartedAt: time.Now()})
	waitFor(t, func() bool { return len(f.framesOf("call.started")) == 1 })
	b.Close()
	waitFor(t, func() bool { return len(f.framesOf("core.bye")) == 1 })
	if len(f.framesOf("call.cancelled")) != 1 {
		t.Fatalf("an unfinished call is cancelled on shutdown")
	}
}

func TestThinlineCoreSkipsUnstreamedTalkgroup(t *testing.T) {
	b := newThinlineFor(t, "http://127.0.0.1:1")
	b.CallStarted(trunking.CallStart{Talkgroup: &trunking.TalkGroup{Stream: false}, DeviceSerial: "x", StartedAt: time.Now()})
	b.mu.Lock()
	n := len(b.started)
	b.mu.Unlock()
	if n != 0 {
		t.Fatalf("a talkgroup opted out of streaming is never reported")
	}
}

func TestThinlineCoreValidatesConfig(t *testing.T) {
	for _, cfg := range []ThinlineCoreConfig{
		{URL: "thinline.example", APIKey: "k", SystemID: 1},
		{URL: "https://thinline.example", SystemID: 1},
		{URL: "https://thinline.example", APIKey: "k"},
	} {
		if _, err := NewThinlineCore(cfg); err == nil {
			t.Errorf("config %+v must be rejected", cfg)
		}
	}
}

func TestThinlineSocketURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://tl.example/":                  "wss://tl.example/api/sdrtrunk",
		"http://10.0.0.5:3000/api/call-upload": "ws://10.0.0.5:3000/api/sdrtrunk",
	} {
		if got := thinlineSocketURL(thinlineStrip(in)); got != want {
			t.Errorf("socket URL for %q = %q, want %q", in, got, want)
		}
	}
	if !strings.HasPrefix(thinlinePatchString(1, nil), "[") {
		t.Fatal("patch string must be bracketed")
	}
}
