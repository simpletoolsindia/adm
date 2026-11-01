// wdm — a small accelerated download manager for Windows (and anywhere Go runs).
//
// It reimplements the core of IDM's engine: probe a URL, split the file into
// byte ranges, fetch them over N parallel connections, re-split a segment when a
// connection finishes early ("download logic accelerator"), write every segment
// straight into the final file at its offset, and resume via HTTP Range/If-Range.
//
// Pure stdlib, no cgo — cross-compiles to a single self-contained wdm.exe.
//
// Usage:
//   wdm <url> [-o output] [-n connections] [-l limitMBps] [-ua userAgent] [-H "Header: value"]...
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// version is overridden at build time via -ldflags "-X main.version=<tag>".
var version = "dev"

const (
	defaultConnections = 8
	minSizeToSplit     = 1 << 20 // 1 MiB
	minSegmentSize     = 1 << 20 // 1 MiB
	ioChunk            = 256 << 10
	maxRetryPerSegment = 15
)

// ---- segment ---------------------------------------------------------------

type Segment struct {
	Start   int64 `json:"start"`
	End     int64 `json:"end"` // inclusive; may shrink when the tail is stolen for a new connection
	Pos     int64 `json:"pos"` // next byte to fetch
	end     atomic.Int64
	pos     atomic.Int64
	running bool
	retry   int
}

func (s *Segment) remaining() int64 {
	r := s.end.Load() - s.pos.Load() + 1
	if r < 0 {
		return 0
	}
	return r
}
func (s *Segment) complete() bool { return s.pos.Load() > s.end.Load() }

// ---- downloader ------------------------------------------------------------

type Downloader struct {
	url, dest string
	client    *http.Client
	headers   map[string]string
	userAgent string

	total        int64
	acceptRanges bool
	validator    string

	maxConn int
	limit   int64 // bytes/sec, 0 = unlimited
	destDir string
	silent  bool

	mu     sync.Mutex
	segs   []*Segment
	active int
	wg     sync.WaitGroup
	file   *os.File

	downloaded atomic.Int64

	errMu    sync.Mutex
	fatalErr error

	doneOnce sync.Once
	done     chan struct{}

	unknownTotal bool
}

func (d *Downloader) setFatal(err error) {
	d.errMu.Lock()
	if d.fatalErr == nil {
		d.fatalErr = err
	}
	d.errMu.Unlock()
}
func (d *Downloader) fatal() error {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	return d.fatalErr
}

// probe discovers size, range support and a validator (ETag/Last-Modified).
func (d *Downloader) probe(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	req.Header.Set("Range", "bytes=0-0")
	d.applyHeaders(req)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64))

	switch resp.StatusCode {
	case http.StatusPartialContent: // 206
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			if i := strings.LastIndex(cr, "/"); i >= 0 {
				if t, err := strconv.ParseInt(strings.TrimSpace(cr[i+1:]), 10, 64); err == nil {
					d.total = t
				}
			}
		}
		d.acceptRanges = true
	case http.StatusOK: // 200 — no range support
		d.total = resp.ContentLength
		d.acceptRanges = strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes")
	default:
		return fmt.Errorf("server returned HTTP %d", resp.StatusCode)
	}

	if et := resp.Header.Get("ETag"); et != "" {
		d.validator = et
	} else if lm := resp.Header.Get("Last-Modified"); lm != "" {
		d.validator = lm
	}
	if d.total <= 0 {
		d.unknownTotal = true
	}
	if d.dest == "" {
		d.dest = filepath.Join(d.destDir, destFromResponse(d.url, resp))
	}
	return nil
}

func (d *Downloader) applyHeaders(req *http.Request) {
	req.Header.Set("User-Agent", d.userAgent)
	for k, v := range d.headers {
		req.Header.Set(k, v)
	}
}

func (d *Downloader) prepareFile() error {
	f, err := os.OpenFile(d.dest, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	d.file = f
	if d.total > 0 {
		if err := f.Truncate(d.total); err != nil { // preallocate so WriteAt can seek anywhere
			return err
		}
	}
	return nil
}

func (d *Downloader) makeInitialSegments() {
	if !d.acceptRanges || d.total < minSizeToSplit {
		end := int64(1<<62 - 1)
		if d.total > 0 {
			end = d.total - 1
		}
		d.addSegment(&Segment{Start: 0, End: end})
		return
	}
	n := d.total / minSegmentSize
	if n > int64(d.maxConn) {
		n = int64(d.maxConn)
	}
	if n < 1 {
		n = 1
	}
	chunk := d.total / n
	var cursor int64
	for i := int64(0); i < n; i++ {
		end := cursor + chunk - 1
		if i == n-1 {
			end = d.total - 1
		}
		d.addSegment(&Segment{Start: cursor, End: end})
		cursor = end + 1
	}
}

func (d *Downloader) addSegment(s *Segment) {
	s.pos.Store(s.Start)
	s.end.Store(s.End)
	if s.Pos > s.Start { // from a resumed state
		s.pos.Store(s.Pos)
	}
	d.segs = append(d.segs, s)
}

// ---- running ---------------------------------------------------------------

func (d *Downloader) run(ctx context.Context) error {
	if err := d.prepareFile(); err != nil {
		return err
	}
	defer d.file.Close()

	if len(d.segs) == 0 {
		d.makeInitialSegments()
	}
	d.downloaded.Store(d.bytesSoFar())

	d.mu.Lock()
	start := d.segs
	for _, s := range start {
		if d.active < d.maxConn {
			d.spawn(ctx, s)
		}
	}
	d.mu.Unlock()

	if !d.silent {
		go d.progressLoop(ctx)
	}

	waitDone := make(chan struct{})
	go func() { d.wg.Wait(); close(waitDone) }()

	select {
	case <-d.done:
	case <-waitDone:
	case <-ctx.Done():
	}
	d.wg.Wait()

	if err := d.fatal(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// spawn starts a worker for seg. Caller must hold d.mu.
func (d *Downloader) spawn(ctx context.Context, seg *Segment) {
	if seg.running || seg.complete() {
		return
	}
	seg.running = true
	d.active++
	d.wg.Add(1)
	go d.worker(ctx, seg)
}

func (d *Downloader) worker(ctx context.Context, seg *Segment) {
	defer d.wg.Done()
	err := d.fetch(ctx, seg)
	d.onFinish(ctx, seg, err)
}

func (d *Downloader) fetch(ctx context.Context, seg *Segment) error {
	from, to := seg.pos.Load(), seg.end.Load()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, d.url, nil)
	d.applyHeaders(req)
	if d.acceptRanges && d.total > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", from, to))
		if d.validator != "" {
			req.Header.Set("If-Range", d.validator)
		}
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if d.acceptRanges && d.total > 0 && resp.StatusCode == http.StatusOK {
		return errors.New("file changed on server (If-Range failed); restart required")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 206 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	buf := make([]byte, ioChunk)
	var winStart = time.Now()
	var winBytes int64
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			pos := seg.pos.Load()
			end := seg.end.Load()
			w := int64(n)
			if d.total > 0 {
				allowed := end - pos + 1
				if allowed <= 0 {
					return nil // our tail was stolen; we're done
				}
				if w > allowed {
					w = allowed
				}
			}
			if _, werr := d.file.WriteAt(buf[:w], pos); werr != nil {
				return werr
			}
			seg.pos.Add(w)
			d.downloaded.Add(w)
			if d.total > 0 && seg.pos.Load() > end {
				return nil
			}
			if err := d.throttle(ctx, &winStart, &winBytes, w); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			if d.unknownTotal {
				d.finalizeStream(seg)
			}
			return nil
		}
		if rerr != nil {
			return rerr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// finalizeStream fixes up size/end for an unknown-length download that just hit EOF.
func (d *Downloader) finalizeStream(seg *Segment) {
	final := seg.pos.Load()
	d.mu.Lock()
	if d.total <= 0 {
		d.total = final
		d.file.Truncate(final)
	}
	d.mu.Unlock()
	seg.end.Store(final - 1) // pos == final ⇒ complete
}

func (d *Downloader) onFinish(ctx context.Context, seg *Segment, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	seg.running = false
	d.active--

	if err != nil && !errors.Is(err, context.Canceled) {
		if strings.Contains(err.Error(), "file changed on server") {
			d.setFatal(err)
		} else {
			seg.retry++
			if seg.retry > maxRetryPerSegment {
				d.setFatal(fmt.Errorf("segment failed after %d retries: %w", maxRetryPerSegment, err))
			}
		}
	}

	if seg.complete() {
		d.removeSegment(seg)
	}

	if d.fatal() != nil {
		if d.active == 0 {
			d.finish()
		}
		return
	}
	if ctx.Err() != nil {
		if d.active == 0 {
			d.finish()
		}
		return
	}

	d.rebalance(ctx)

	if len(d.segs) == 0 {
		d.finish()
	}
}

// rebalance keeps up to maxConn connections busy: first revive any segment that
// lost its worker, then split the busiest segment to use a free connection slot.
// Caller holds d.mu.
func (d *Downloader) rebalance(ctx context.Context) {
	for _, s := range d.segs {
		if d.active >= d.maxConn {
			return
		}
		if !s.running && !s.complete() {
			d.spawn(ctx, s)
		}
	}
	if !d.acceptRanges || d.total <= 0 {
		return
	}
	for d.active < d.maxConn {
		var busiest *Segment
		for _, s := range d.segs {
			if busiest == nil || s.remaining() > busiest.remaining() {
				busiest = s
			}
		}
		if busiest == nil || busiest.remaining() <= 2*minSegmentSize {
			return
		}
		half := busiest.remaining() / 2
		oldEnd := busiest.end.Load()
		newStart := oldEnd - half + 1
		busiest.end.Store(newStart - 1) // running worker stops when it reaches here
		ns := &Segment{Start: newStart, End: oldEnd}
		ns.pos.Store(newStart)
		ns.end.Store(oldEnd)
		d.segs = append(d.segs, ns)
		d.spawn(ctx, ns)
	}
}

// removeSegment drops a completed segment. Caller holds d.mu.
func (d *Downloader) removeSegment(seg *Segment) {
	for i, s := range d.segs {
		if s == seg {
			d.segs = append(d.segs[:i], d.segs[i+1:]...)
			return
		}
	}
}

func (d *Downloader) finish() { d.doneOnce.Do(func() { close(d.done) }) }

func (d *Downloader) throttle(ctx context.Context, winStart *time.Time, winBytes *int64, added int64) error {
	if d.limit <= 0 {
		return nil
	}
	*winBytes += added
	elapsed := time.Since(*winStart).Seconds()
	allowed := float64(d.limit) * max(elapsed, 0.001)
	if float64(*winBytes) > allowed {
		over := float64(*winBytes) - allowed
		sleep := time.Duration(over / float64(d.limit) * float64(time.Second))
		t := time.NewTimer(sleep)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if elapsed > 1 {
		*winStart = time.Now()
		*winBytes = 0
	}
	return nil
}

func (d *Downloader) bytesSoFar() int64 {
	var sum int64
	for _, s := range d.segs {
		sum += s.pos.Load() - s.Start
	}
	return sum
}

// ---- progress + state ------------------------------------------------------

func (d *Downloader) progressLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last int64
	lastT := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.done:
			return
		case <-ticker.C:
			cur := d.downloaded.Load()
			now := time.Now()
			bps := float64(cur-last) / now.Sub(lastT).Seconds()
			last, lastT = cur, now
			d.mu.Lock()
			conns := d.active
			d.mu.Unlock()
			printProgress(cur, d.total, bps, conns)
		}
	}
}

type state struct {
	URL      string     `json:"url"`
	Dest     string     `json:"dest"`
	Total    int64      `json:"total"`
	Ranges   bool       `json:"ranges"`
	Valid    string     `json:"validator"`
	Segments []*Segment `json:"segments"`
}

func (d *Downloader) stateFile() string { return d.dest + ".wdmstate" }

func (d *Downloader) saveState() {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := state{URL: d.url, Dest: d.dest, Total: d.total, Ranges: d.acceptRanges, Valid: d.validator}
	for _, s := range d.segs {
		st.Segments = append(st.Segments, &Segment{Start: s.Start, End: s.end.Load(), Pos: s.pos.Load()})
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err == nil {
		os.WriteFile(d.stateFile(), b, 0o644)
	}
}

func (d *Downloader) loadState() bool {
	b, err := os.ReadFile(d.stateFile())
	if err != nil {
		return false
	}
	var st state
	if json.Unmarshal(b, &st) != nil {
		return false
	}
	d.total = st.Total
	d.acceptRanges = st.Ranges
	d.validator = st.Valid
	d.unknownTotal = st.Total <= 0
	for _, s := range st.Segments {
		if s.Pos <= s.End || st.Total <= 0 {
			d.addSegment(s)
		}
	}
	return len(d.segs) > 0
}

// ---- helpers ---------------------------------------------------------------

func destFromResponse(rawURL string, resp *http.Response) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if i := strings.Index(strings.ToLower(cd), "filename="); i >= 0 {
			name := cd[i+len("filename="):]
			name = strings.Trim(name, "\"' ;")
			if j := strings.IndexAny(name, ";"); j >= 0 {
				name = name[:j]
			}
			if name != "" {
				return filepath.Base(name)
			}
		}
	}
	u := rawURL
	if i := strings.IndexAny(u, "?#"); i >= 0 {
		u = u[:i]
	}
	base := filepath.Base(u)
	if base == "" || base == "." || base == "/" {
		base = fmt.Sprintf("download-%d", time.Now().Unix())
	}
	return base
}

func humanBytes(n int64) string {
	if n < 0 {
		return "?"
	}
	const u = "KMGTP"
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	v := float64(n)
	i := -1
	for v >= 1024 && i < len(u)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.2f %ciB", v, u[i])
}

func printProgress(cur, total int64, bps float64, conns int) {
	speed := humanBytes(int64(bps)) + "/s"
	if total > 0 {
		pct := int(float64(cur) / float64(total) * 100)
		filled := pct / 5
		bar := strings.Repeat("#", filled) + strings.Repeat("-", 20-filled)
		eta := "?"
		if bps > 1 {
			secs := float64(total-cur) / bps
			eta = (time.Duration(secs) * time.Second).Truncate(time.Second).String()
		}
		fmt.Fprintf(os.Stderr, "\r[%s] %3d%%  %s / %s  %s  %d conn  ETA %s    ",
			bar, pct, humanBytes(cur), humanBytes(total), speed, conns, eta)
	} else {
		fmt.Fprintf(os.Stderr, "\r%s downloaded  %s  %d conn    ", humanBytes(cur), speed, conns)
	}
}

type headerFlags map[string]string

func (h headerFlags) String() string { return "" }
func (h headerFlags) Set(v string) error {
	i := strings.Index(v, ":")
	if i < 0 {
		return fmt.Errorf("header must be 'Name: value'")
	}
	h[strings.TrimSpace(v[:i])] = strings.TrimSpace(v[i+1:])
	return nil
}

// options for one download, shared by the direct path and the queue.
type job struct {
	URL  string `json:"url"`
	Dir  string `json:"dir,omitempty"`
	Name string `json:"name,omitempty"`
	Out  string `json:"out,omitempty"`
}

type cliOpts struct {
	conns   int
	limit   int64
	ua      string
	headers map[string]string
	silent  bool
}

func main() {
	var (
		add        = flag.String("add", "", "URL to download")
		dir        = flag.String("dir", "", "output directory")
		name       = flag.String("name", "", "output file name")
		out        = flag.String("o", "", "full output path (overrides --dir/--name)")
		silent     = flag.Bool("silent", false, "no progress output")
		later      = flag.Bool("later", false, "add to the queue instead of downloading now")
		startQueue = flag.Bool("start-queue", false, "download everything queued with --later")
		conns      = flag.Int("n", defaultConnections, "max parallel connections (also --connections)")
		limitMB    = flag.Float64("l", 0, "speed limit in MB/s (0 = unlimited)")
		ua         = flag.String("ua", "adm/"+version, "User-Agent")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.IntVar(conns, "connections", defaultConnections, "max parallel connections")
	headers := headerFlags{}
	flag.Var(headers, "H", "extra request header 'Name: value' (repeatable)")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "ADM — Anything Download Manager\n\nusage:")
		fmt.Fprintln(os.Stderr, "  adm <url> [--dir PATH] [--name FILE] [-o OUT] [-n N] [-l MBps] [--silent]")
		fmt.Fprintln(os.Stderr, "  adm --add <url> [--dir PATH] [--name FILE] [--later] [--silent]")
		fmt.Fprintln(os.Stderr, "  adm --start-queue")
		fmt.Fprintln(os.Stderr, "\nflags:")
		flag.PrintDefaults()
	}

	// Allow the URL before or after the flags: "adm <url> -n 8" and "adm -n 8 <url>".
	var posURL string
	rest := os.Args[1:]
	if len(rest) > 0 && strings.HasPrefix(rest[0], "-") {
		flag.Parse()
		posURL = flag.Arg(0)
	} else if len(rest) > 0 {
		posURL = rest[0]
		flag.CommandLine.Parse(rest[1:])
	}

	if *showVer {
		fmt.Printf("adm %s\n", version)
		return
	}

	opts := cliOpts{conns: *conns, limit: int64(*limitMB * 1_000_000), ua: *ua, headers: headers, silent: *silent}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *startQueue {
		if err := runQueue(ctx, opts); err != nil {
			fmt.Fprintf(os.Stderr, "queue: %v\n", err)
			os.Exit(1)
		}
		return
	}

	url := *add
	if url == "" {
		url = posURL
	}
	if url == "" {
		flag.Usage()
		os.Exit(2)
	}
	j := job{URL: url, Dir: *dir, Name: *name, Out: *out}

	if *later {
		if err := enqueue(j); err != nil {
			fmt.Fprintf(os.Stderr, "could not queue: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "queued: %s\n", url)
		return
	}

	if err := download(ctx, j, opts); err != nil {
		if errors.Is(err, context.Canceled) {
			os.Exit(130)
		}
		os.Exit(1)
	}
}

// download runs a single job to completion (or resumes it).
func download(ctx context.Context, j job, o cliOpts) error {
	d := &Downloader{
		url:       j.URL,
		dest:      j.Out,
		destDir:   j.Dir,
		headers:   o.headers,
		userAgent: o.ua,
		maxConn:   o.conns,
		limit:     o.limit,
		silent:    o.silent,
		done:      make(chan struct{}),
		client: &http.Client{
			Transport: &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 256},
		},
	}
	if j.Name != "" && j.Out == "" {
		d.dest = filepath.Join(j.Dir, j.Name)
	}

	resumed := false
	if d.dest != "" {
		resumed = d.loadState()
	}
	if !resumed {
		if !d.silent {
			fmt.Fprintf(os.Stderr, "probing %s ...\n", j.URL)
		}
		if err := d.probe(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "probe failed: %v\n", err)
			return err
		}
		if d.loadState() {
			resumed = true
		}
	}
	if !d.silent {
		fmt.Fprintf(os.Stderr, "→ %s   size=%s   ranges=%v   connections=%d%s\n",
			d.dest, humanBytes(d.total), d.acceptRanges, d.maxConn,
			map[bool]string{true: "   (resuming)", false: ""}[resumed])
	}

	err := d.run(ctx)
	if !d.silent {
		fmt.Fprintln(os.Stderr)
	}
	if err != nil {
		d.saveState()
		if errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "paused — state saved to %s (re-run to resume)\n", d.stateFile())
		} else {
			fmt.Fprintf(os.Stderr, "failed: %v\n", err)
		}
		return err
	}
	os.Remove(d.stateFile())
	if !d.silent {
		fmt.Fprintf(os.Stderr, "done: %s (%s)\n", d.dest, humanBytes(d.total))
	}
	return nil
}

// ---- queue -----------------------------------------------------------------

func queuePath() string {
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = "."
	}
	dir := filepath.Join(base, "adm")
	os.MkdirAll(dir, 0o755)
	return filepath.Join(dir, "queue.jsonl")
}

func enqueue(j job) error {
	f, err := os.OpenFile(queuePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, _ := json.Marshal(j)
	_, err = f.Write(append(b, '\n'))
	return err
}

func runQueue(ctx context.Context, o cliOpts) error {
	path := queuePath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(os.Stderr, "queue is empty")
			return nil
		}
		return err
	}
	var jobs []job
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var j job
		if json.Unmarshal([]byte(line), &j) == nil && j.URL != "" {
			jobs = append(jobs, j)
		}
	}
	if len(jobs) == 0 {
		fmt.Fprintln(os.Stderr, "queue is empty")
		return nil
	}
	fmt.Fprintf(os.Stderr, "starting queue: %d item(s)\n", len(jobs))
	var remaining []job
	var firstErr error
	for i, j := range jobs {
		if ctx.Err() != nil {
			remaining = append(remaining, jobs[i:]...)
			break
		}
		fmt.Fprintf(os.Stderr, "[%d/%d] %s\n", i+1, len(jobs), j.URL)
		if err := download(ctx, j, o); err != nil {
			if errors.Is(err, context.Canceled) {
				remaining = append(remaining, jobs[i:]...)
				break
			}
			if firstErr == nil {
				firstErr = err
			}
			// keep failed item in the queue for a later retry
			remaining = append(remaining, j)
		}
	}
	// rewrite the queue with whatever didn't finish
	if len(remaining) == 0 {
		os.Remove(path)
	} else {
		var buf strings.Builder
		for _, j := range remaining {
			b, _ := json.Marshal(j)
			buf.Write(b)
			buf.WriteByte('\n')
		}
		os.WriteFile(path, []byte(buf.String()), 0o644)
	}
	return firstErr
}
