package broadcast

// Thinline Radio Core: a site-held call stream ported from N9KOP/sdrtrunk.
//
// Instead of uploading every finished call, the site keeps each recording
// and reports it to Thinline Radio over a websocket (call.started when a
// voice SDR picks the call up, call.ready once the recording is written).
// The server picks which site's copy it wants and sends an uploadCall (or
// discardCall) command back. When the websocket is down, held calls are
// uploaded directly so nothing is lost.
//
// Wire protocol (JSON text frames on <url>/api/sdrtrunk):
//
//	site -> server  {"type":"hello","key":<site key>}
//	server -> site  {"type":"welcome"}                       (ready)
//	site -> server  {"type":"core.hello","system":<id>}
//	site -> server  {"type":"call.started"|"call.ready", recordingId, system,
//	                 talkgroup, radioId, frequency, start (ms), duration (ms),
//	                 patches [..]}
//	site -> server  {"type":"call.cancelled"|"call.uploadFailed", recordingId, system}
//	server -> site  {"type":"command","command":"uploadCall"|"discardCall",
//	                 "recordingId":..., "id":...}
//	site -> server  {"type":"result","id":...,"ok":bool,"error":"..."}
//	site -> server  {"type":"core.bye","system":<id>}
//
// Uploads go to <url>/api/call-upload as multipart form data, and count as
// done only when the response is HTTP 200 containing "Call imported
// successfully.".

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MattCheramie/GopherTrunk/internal/trunking"
)

const (
	thinlineUploadPath = "/api/call-upload"
	thinlineSocketPath = "/api/sdrtrunk"
	thinlineImportedOK = "Call imported successfully."
	// Both upload attempts together stay under the server's 15 second wait
	// for a requested upload.
	thinlineUploadAttempts = 2
	thinlineUploadTimeout  = 6 * time.Second
	// A call reported as started but never completed (too short, stream
	// opted out, recorder dropped it) is cancelled this long after it ends.
	thinlineCancelGrace = 20 * time.Second
)

// ThinlineCoreConfig configures one Thinline Radio Core stream.
type ThinlineCoreConfig struct {
	// Name is an optional log label; defaults to "thinline-core".
	Name string
	// URL is the Thinline Radio base URL (http:// or https://). A trailing
	// /api/call-upload is accepted and stripped.
	URL string
	// APIKey is the site control key (not an upload key).
	APIKey string
	// SystemID is the numeric Thinline Radio system ID.
	SystemID int
	// Systems restricts the stream to these trunking-system names.
	Systems []string
	Log     *slog.Logger
}

type thinlineStage int

const (
	thinlineHeld thinlineStage = iota
	thinlineUploading
	thinlineDone
)

type thinlineHeldCall struct {
	call  *Call
	stage thinlineStage
}

type thinlineStarted struct {
	id        string
	grant     trunking.Grant
	startedAt time.Time
	cancel    *time.Timer
}

type thinlineCore struct {
	systemFilter
	name     string
	base     string
	key      string
	systemID int
	log      *slog.Logger

	http      *http.Client
	newClient func() *http.Client
	dial      func(ctx context.Context, url string) (*websocket.Conn, error)

	mu         sync.Mutex
	conn       *websocket.Conn
	ready      bool
	connecting bool
	stopped    bool
	started    map[string]*thinlineStarted // call key -> started call
	held       map[string]*thinlineHeldCall

	writeMu sync.Mutex
}

// NewThinlineCore builds a Thinline Radio Core backend.
func NewThinlineCore(cfg ThinlineCoreConfig) (Backend, error) {
	base := thinlineStrip(cfg.URL)
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, errors.New("broadcast/thinline_core: url must start with http:// or https://")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("broadcast/thinline_core: api_key (site control key) is required")
	}
	if cfg.SystemID == 0 {
		return nil, errors.New("broadcast/thinline_core: system_id is required")
	}
	name := cfg.Name
	if name == "" {
		name = "thinline-core"
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	b := &thinlineCore{
		systemFilter: newSystemFilter(cfg.Systems),
		name:         name,
		base:         base,
		key:          strings.TrimSpace(cfg.APIKey),
		systemID:     cfg.SystemID,
		log:          log,
		newClient:    thinlineHTTPClient,
		started:      make(map[string]*thinlineStarted),
		held:         make(map[string]*thinlineHeldCall),
	}
	b.http = b.newClient()
	b.dial = func(ctx context.Context, url string) (*websocket.Conn, error) {
		d := websocket.Dialer{HandshakeTimeout: 10 * time.Second, Proxy: http.ProxyFromEnvironment}
		c, _, err := d.DialContext(ctx, url, nil)
		return c, err
	}
	return b, nil
}

func thinlineHTTPClient() *http.Client {
	// A fresh Transport per client, so the retry goes out on a new
	// connection instead of a pooled one the network silently dropped.
	return &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
}

func (b *thinlineCore) Name() string { return b.name }

// callKey ties a call's start to its finished recording: the voice SDR
// serial plus the call's start instant.
func thinlineKey(serial string, start time.Time) string {
	return serial + "|" + strconv.FormatInt(start.UnixNano(), 10)
}

func newRecordingID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// CallStarted reports a call a voice SDR has just picked up.
func (b *thinlineCore) CallStarted(cs trunking.CallStart) {
	if cs.Talkgroup != nil && !cs.Talkgroup.Stream {
		return
	}
	key := thinlineKey(cs.DeviceSerial, cs.StartedAt)
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return
	}
	if _, ok := b.started[key]; ok {
		b.mu.Unlock()
		return
	}
	st := &thinlineStarted{id: newRecordingID(), grant: cs.Grant, startedAt: cs.StartedAt}
	b.started[key] = st
	ready := b.ready
	b.mu.Unlock()

	b.ensureConnected()
	if ready {
		b.send(b.report("call.started", st.id, st.grant, st.startedAt, 0))
	}
}

// CallEnded arms a cancel for a started call whose recording never
// reaches Send.
func (b *thinlineCore) CallEnded(ce trunking.CallEnd) {
	key := thinlineKey(ce.DeviceSerial, ce.StartedAt)
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.started[key]
	if !ok || st.cancel != nil {
		return
	}
	st.cancel = time.AfterFunc(thinlineCancelGrace, func() {
		b.mu.Lock()
		cur, ok := b.started[key]
		if !ok || cur != st {
			b.mu.Unlock()
			return
		}
		delete(b.started, key)
		b.mu.Unlock()
		b.sendSimple("call.cancelled", st.id)
	})
}

// Send holds a finished recording and reports it as ready. The upload
// happens when the server asks for it, or directly if the server can't be
// reached.
func (b *thinlineCore) Send(ctx context.Context, c *Call) error {
	start := c.CallStartedAt
	if start.IsZero() {
		start = c.StartedAt
	}
	key := thinlineKey(c.DeviceSerial, start)

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return errors.New(b.name + ": stopped")
	}
	id := ""
	// Only the call's first recording takes the started call's ID; later
	// per-over segments are their own recordings.
	if st, ok := b.started[key]; ok && c.Segment == 0 {
		id = st.id
		if st.cancel != nil {
			st.cancel.Stop()
		}
		delete(b.started, key)
	}
	if id == "" {
		id = newRecordingID()
	}
	h := &thinlineHeldCall{call: c}
	b.held[id] = h
	ready := b.ready
	b.mu.Unlock()

	b.ensureConnected()
	if ready {
		b.send(b.readyReport(id, c))
	}
	return nil
}

// Close cancels started calls, says goodbye and uploads whatever is still
// held so a shutdown loses nothing.
func (b *thinlineCore) Close() error {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return nil
	}
	b.stopped = true
	var cancelled []string
	for k, st := range b.started {
		if st.cancel != nil {
			st.cancel.Stop()
		}
		cancelled = append(cancelled, st.id)
		delete(b.started, k)
	}
	b.mu.Unlock()

	for _, id := range cancelled {
		b.sendSimple("call.cancelled", id)
	}
	b.send(map[string]any{"type": "core.bye", "system": b.systemID})

	b.mu.Lock()
	conn := b.conn
	b.conn, b.ready = nil, false
	b.mu.Unlock()
	if conn != nil {
		b.writeMu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second))
		b.writeMu.Unlock()
		_ = conn.Close()
	}
	b.uploadAllHeld()
	return nil
}

// HeldCount reports how many recordings are waiting on the server.
func (b *thinlineCore) HeldCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.held)
}

func (b *thinlineCore) ensureConnected() {
	b.mu.Lock()
	if b.stopped || b.conn != nil || b.connecting {
		b.mu.Unlock()
		return
	}
	b.connecting = true
	b.mu.Unlock()
	go b.connect()
}

func (b *thinlineCore) connect() {
	url := thinlineSocketURL(b.base)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	conn, err := b.dial(ctx, url)
	cancel()
	if err == nil {
		b.writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err = conn.WriteJSON(map[string]any{"type": "hello", "key": b.key})
		b.writeMu.Unlock()
		if err != nil {
			_ = conn.Close()
		}
	}
	b.mu.Lock()
	b.connecting = false
	if err != nil || b.stopped {
		b.mu.Unlock()
		if err != nil {
			b.log.Warn("thinline: websocket connect failed; uploading held calls directly",
				"backend", b.name, "url", url, "err", err)
			b.uploadAllHeld()
		} else {
			_ = conn.Close()
		}
		return
	}
	b.conn = conn
	b.mu.Unlock()
	go b.readLoop(conn)
}

func (b *thinlineCore) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			b.disconnected(conn, err)
			return
		}
		var msg struct {
			Type        string `json:"type"`
			Command     string `json:"command"`
			RecordingID string `json:"recordingId"`
			ID          any    `json:"id"`
			Error       string `json:"error"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "welcome":
			b.mu.Lock()
			if b.conn == conn {
				b.ready = true
			}
			b.mu.Unlock()
			b.announce()
		case "error":
			b.log.Warn("thinline: server refused the site", "backend", b.name, "error", msg.Error)
		case "command":
			b.onCommand(msg.Command, msg.RecordingID, msg.ID)
		}
	}
}

func (b *thinlineCore) disconnected(conn *websocket.Conn, err error) {
	b.mu.Lock()
	if b.conn == conn {
		b.conn, b.ready = nil, false
	}
	stopped := b.stopped
	b.mu.Unlock()
	_ = conn.Close()
	if stopped {
		return
	}
	b.log.Warn("thinline: websocket closed; uploading held calls directly", "backend", b.name, "err", err)
	b.uploadAllHeld()
}

// announce tells a freshly welcomed server about every call in flight.
func (b *thinlineCore) announce() {
	b.send(map[string]any{"type": "core.hello", "system": b.systemID})
	b.mu.Lock()
	var msgs []map[string]any
	for _, st := range b.started {
		msgs = append(msgs, b.report("call.started", st.id, st.grant, st.startedAt, 0))
	}
	for id, h := range b.held {
		if h.stage == thinlineHeld {
			msgs = append(msgs, b.readyReport(id, h.call))
		}
	}
	b.mu.Unlock()
	for _, m := range msgs {
		b.send(m)
	}
}

func (b *thinlineCore) onCommand(command, recordingID string, id any) {
	if command != "uploadCall" && command != "discardCall" {
		return
	}
	b.mu.Lock()
	h, ok := b.held[recordingID]
	if ok && command == "discardCall" {
		h.stage = thinlineDone
		delete(b.held, recordingID)
	}
	b.mu.Unlock()
	errText := ""
	if !ok {
		errText = "Recording is not held"
	}
	if ok && command == "uploadCall" {
		go b.upload(recordingID)
	}
	b.send(map[string]any{"type": "result", "id": id, "ok": ok, "error": errText})
}

func (b *thinlineCore) uploadAllHeld() {
	b.mu.Lock()
	ids := make([]string, 0, len(b.held))
	for id := range b.held {
		ids = append(ids, id)
	}
	b.mu.Unlock()
	for _, id := range ids {
		b.upload(id)
	}
}

func (b *thinlineCore) upload(recordingID string) {
	b.mu.Lock()
	h, ok := b.held[recordingID]
	if !ok || h.stage != thinlineHeld {
		b.mu.Unlock()
		return
	}
	h.stage = thinlineUploading
	b.mu.Unlock()

	err := b.post(recordingID, h.call)

	b.mu.Lock()
	if err == nil {
		h.stage = thinlineDone
		delete(b.held, recordingID)
		b.mu.Unlock()
		return
	}
	h.stage = thinlineHeld
	b.mu.Unlock()
	b.log.Warn("thinline: upload failed", "backend", b.name, "recordingId", recordingID, "err", err)
	// Tell the server, so it asks another site's copy.
	b.sendSimple("call.uploadFailed", recordingID)
}

func (b *thinlineCore) post(recordingID string, c *Call) error {
	audio, err := c.MP3()
	if err != nil {
		return fmt.Errorf("encode mp3: %w", err)
	}
	name := audioFilename(c, "mp3")
	fields := []multipartField{
		{Name: "key", Value: b.key},
		{Name: "system", Value: strconv.Itoa(b.systemID)},
		{Name: "talkgroup", Value: strconv.FormatUint(uint64(c.Talkgroup), 10)},
		{Name: "source", Value: strconv.FormatUint(uint64(c.Source), 10)},
		{Name: "frequency", Value: strconv.FormatUint(uint64(c.FrequencyHz), 10)},
		{Name: "dateTime", Value: strconv.FormatInt(c.StartedAt.UnixMilli(), 10)},
		{Name: "duration", Value: strconv.FormatInt(c.Duration().Milliseconds(), 10)},
		{Name: "recordingId", Value: recordingID},
		{Name: "patches", Value: thinlinePatchString(c.Talkgroup, c.PatchedGroups)},
		{Name: "audioName", Value: name},
		{Name: "audioType", Value: "audio/mpeg"},
		{Name: "audio", Filename: name, Data: audio},
	}
	if c.TalkgroupLabel != "" {
		fields = append(fields, multipartField{Name: "talkgroupLabel", Value: c.TalkgroupLabel})
	}

	var lastErr error
	for attempt := 1; attempt <= thinlineUploadAttempts; attempt++ {
		client := b.http
		if attempt > 1 {
			client = b.newClient()
		}
		body, contentType, err := buildMultipart(fields)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), thinlineUploadTimeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.base+thinlineUploadPath, body)
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("User-Agent", "trunk-suite")
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			if attempt > 1 {
				client.CloseIdleConnections()
			}
			continue
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		cancel()
		if attempt > 1 {
			client.CloseIdleConnections()
		}
		if resp.StatusCode == http.StatusOK && strings.Contains(string(msg), thinlineImportedOK) {
			return nil
		}
		lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
		if resp.StatusCode < 500 {
			break
		}
	}
	return lastErr
}

func (b *thinlineCore) report(typ, id string, g trunking.Grant, start time.Time, duration time.Duration) map[string]any {
	return map[string]any{
		"type":        typ,
		"recordingId": id,
		"system":      b.systemID,
		"talkgroup":   g.GroupID,
		"radioId":     g.SourceID,
		"frequency":   g.FrequencyHz,
		"start":       start.UnixMilli(),
		"duration":    duration.Milliseconds(),
		"patches":     thinlinePatches(g.GroupID, g.PatchedGroups),
	}
}

func (b *thinlineCore) readyReport(id string, c *Call) map[string]any {
	return map[string]any{
		"type":        "call.ready",
		"recordingId": id,
		"system":      b.systemID,
		"talkgroup":   c.Talkgroup,
		"radioId":     c.Source,
		"frequency":   c.FrequencyHz,
		"start":       c.StartedAt.UnixMilli(),
		"duration":    c.Duration().Milliseconds(),
		"patches":     thinlinePatches(c.Talkgroup, c.PatchedGroups),
	}
}

func (b *thinlineCore) sendSimple(typ, id string) {
	b.send(map[string]any{"type": typ, "recordingId": id, "system": b.systemID})
}

// send writes one JSON frame if the server has welcomed this site.
func (b *thinlineCore) send(msg map[string]any) bool {
	b.mu.Lock()
	conn, ready := b.conn, b.ready
	b.mu.Unlock()
	if conn == nil || !ready {
		return false
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return conn.WriteJSON(msg) == nil
}

// thinlinePatches lists a patch super-group followed by its members, or
// nothing for an unpatched call.
func thinlinePatches(group uint32, members []uint32) []uint32 {
	if len(members) == 0 {
		return []uint32{}
	}
	out := make([]uint32, 0, len(members)+1)
	out = append(out, group)
	for _, m := range members {
		if m != 0 {
			out = append(out, m)
		}
	}
	return out
}

func thinlinePatchString(group uint32, members []uint32) string {
	p := thinlinePatches(group, members)
	parts := make([]string, len(p))
	for i, v := range p {
		parts[i] = strconv.FormatUint(uint64(v), 10)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func thinlineStrip(u string) string {
	s := strings.TrimSpace(u)
	s = strings.TrimSuffix(s, thinlineUploadPath)
	return strings.TrimRight(s, "/")
}

func thinlineSocketURL(base string) string {
	switch {
	case strings.HasPrefix(base, "https://"):
		return "wss://" + strings.TrimPrefix(base, "https://") + thinlineSocketPath
	case strings.HasPrefix(base, "http://"):
		return "ws://" + strings.TrimPrefix(base, "http://") + thinlineSocketPath
	}
	return base
}
