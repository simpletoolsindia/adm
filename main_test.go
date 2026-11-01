package main

import (
	"net/http"
	"testing"
)

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:          "0 B",
		512:        "512 B",
		1024:       "1.00 KiB",
		1536:       "1.50 KiB",
		1 << 20:    "1.00 MiB",
		16727283:   "15.95 MiB",
	}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestSegment(t *testing.T) {
	s := &Segment{Start: 100, End: 199}
	s.end.Store(199)
	s.pos.Store(100)
	if s.remaining() != 100 || s.complete() {
		t.Fatalf("fresh segment wrong: rem=%d complete=%v", s.remaining(), s.complete())
	}
	s.pos.Store(200)
	if !s.complete() || s.remaining() != 0 {
		t.Fatalf("finished segment wrong: rem=%d complete=%v", s.remaining(), s.complete())
	}
}

func TestInitialSegmentsCoverFile(t *testing.T) {
	d := &Downloader{total: 10 * minSegmentSize, acceptRanges: true, maxConn: 4}
	d.makeInitialSegments()
	if len(d.segs) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(d.segs))
	}
	// Segments must tile [0, total) with no gaps or overlaps.
	var cursor int64
	for i, s := range d.segs {
		if s.Start != cursor {
			t.Fatalf("segment %d starts at %d, expected %d", i, s.Start, cursor)
		}
		cursor = s.End + 1
	}
	if cursor != d.total {
		t.Fatalf("segments end at %d, expected %d", cursor, d.total)
	}
}

func TestSmallFileSingleConnection(t *testing.T) {
	d := &Downloader{total: 1000, acceptRanges: true, maxConn: 8}
	d.makeInitialSegments()
	if len(d.segs) != 1 {
		t.Fatalf("small file should use 1 segment, got %d", len(d.segs))
	}
}

func TestDestFromResponse(t *testing.T) {
	r := &http.Response{Header: http.Header{}}
	r.Header.Set("Content-Disposition", `attachment; filename="cool file.iso"`)
	if got := destFromResponse("https://x/y?z=1", r); got != "cool file.iso" {
		t.Errorf("content-disposition name = %q", got)
	}
	r2 := &http.Response{Header: http.Header{}}
	if got := destFromResponse("https://host/path/app-1.2.3.exe?token=abc", r2); got != "app-1.2.3.exe" {
		t.Errorf("url-derived name = %q", got)
	}
}
