package antch

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestCrawler builds a crawler wired to an item channel with fast,
// deterministic timing for regression tests.
func newTestCrawler() *Crawler {
	c := NewCrawler()
	c.DownloadDelay = time.Millisecond
	c.MaxConcurrentRequests = 8
	c.MaxConcurrentRequestsPerSite = 2
	return c
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q failed: %v", raw, err)
	}
	return u
}

func TestSiteKeyNormalization(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"plain http host", "http://example.com/path", "http://example.com:80"},
		{"explicit default http port", "http://example.com:80/path", "http://example.com:80"},
		{"plain https host", "https://example.com/path", "https://example.com:443"},
		{"explicit default https port", "https://example.com:443/path", "https://example.com:443"},
		{"upper case host", "http://EXAMPLE.COM/path", "http://example.com:80"},
		{"mixed case host", "http://ExAmPle.CoM:8080/x", "http://example.com:8080"},
		{"upper case scheme", "HTTP://example.com/x", "http://example.com:80"},
		{"non default port", "http://example.com:8080/x", "http://example.com:8080"},
		{"same host different scheme", "https://example.com/x", "https://example.com:443"},
		{"ipv6 no port", "http://[2001:db8::1]/x", "http://[2001:db8::1]:80"},
		{"ipv6 default port", "http://[2001:db8::1]:80/x", "http://[2001:db8::1]:80"},
		{"ipv6 explicit port", "http://[2001:db8::1]:8080/x", "http://[2001:db8::1]:8080"},
		{"ipv6 https no port", "https://[fe80::1]/x", "https://[fe80::1]:443"},
		{"unknown scheme without port", "ftp://example.com/x", "ftp://example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := siteKey(mustParseURL(t, tt.raw))
			if err != nil {
				t.Fatalf("siteKey(%q) error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("siteKey(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestSiteKeyMissingHost(t *testing.T) {
	if _, err := siteKey(mustParseURL(t, "http:///path")); err == nil {
		t.Error("siteKey with empty host should return an error")
	}
}

func TestSiteKeyOriginIsolation(t *testing.T) {
	// Different schemes and ports on the same host must be distinct
	// sites; implicit and explicit default ports must collapse.
	groups := []struct {
		raws []string
		want string
	}{
		{[]string{"http://a.com/", "http://a.com:80/", "http://A.COM/"}, "http://a.com:80"},
		{[]string{"https://a.com/", "https://a.com:443/"}, "https://a.com:443"},
		{[]string{"http://a.com:8080/"}, "http://a.com:8080"},
		{[]string{"http://[::1]/"}, "http://[::1]:80"},
	}
	seen := make(map[string]string)
	for _, g := range groups {
		for _, raw := range g.raws {
			key, err := siteKey(mustParseURL(t, raw))
			if err != nil {
				t.Fatalf("siteKey(%q): %v", raw, err)
			}
			if key != g.want {
				t.Errorf("siteKey(%q) = %q, want %q", raw, key, g.want)
			}
			if prev, ok := seen[key]; ok && prev != g.want {
				t.Errorf("key %q produced by conflicting groups", key)
			}
			seen[key] = g.want
		}
	}
	if len(seen) != len(groups) {
		t.Errorf("expected %d isolated origins, got %d: %v", len(groups), len(seen), seen)
	}
}

// TestDualPortConcurrency verifies two ports of the same host run with
// independent concurrency budgets: each site must reach its own
// per-site concurrency of 2, and neither waits on the other site's
// gate.
func TestDualPortConcurrency(t *testing.T) {
	newGatedServer := func() (*httptest.Server, chan struct{}, chan struct{}, *int32) {
		gate := make(chan struct{})
		inside := make(chan struct{}, 8)
		var inFlight int32
		var maxInFlight int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cur := atomic.AddInt32(&inFlight, 1)
			defer atomic.AddInt32(&inFlight, -1)
			for {
				old := atomic.LoadInt32(&maxInFlight)
				if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
					break
				}
			}
			inside <- struct{}{}
			<-gate
			w.Write([]byte("ok"))
		}))
		return ts, gate, inside, &maxInFlight
	}
	tsA, gateA, insideA, maxA := newGatedServer()
	tsB, gateB, insideB, maxB := newGatedServer()
	defer tsA.Close()
	defer tsB.Close()

	tc := newTestCrawler()
	// No download delay here: two requests on the same site must be
	// dispatched to both per-site workers at the same time.
	tc.DownloadDelay = 0
	items := make(chan Item, 16)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	// Two requests per server; per-site concurrency is 2, so both
	// requests on each site must enter the handler while its gate is
	// still closed.
	for i := 0; i < 2; i++ {
		if err := tc.EnqueueURL(tsA.URL); err != nil {
			t.Fatal(err)
		}
		if err := tc.EnqueueURL(tsB.URL); err != nil {
			t.Fatal(err)
		}
	}

	waitInside := func(name string, ch chan struct{}) {
		for i := 0; i < 2; i++ {
			select {
			case <-ch:
			case <-time.After(2 * time.Second):
				t.Fatalf("site %s: only %d/2 requests reached the handler (per-site concurrency broken)", name, i)
			}
		}
	}
	waitInside("A", insideA)
	waitInside("B", insideB)

	// Release the gates independently: site B must not depend on A.
	close(gateA)
	close(gateB)
	for i := 0; i < 4; i++ {
		select {
		case v := <-items:
			if v != "ok" {
				t.Errorf("item = %v, want ok", v)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pipeline items")
		}
	}
	if got := atomic.LoadInt32(maxA); got < 2 {
		t.Errorf("port A max concurrency = %d, want >= 2", got)
	}
	if got := atomic.LoadInt32(maxB); got < 2 {
		t.Errorf("port B max concurrency = %d, want >= 2", got)
	}
}

// TestDefaultPortEquivalence verifies a request written with an
// explicit default port reuses the exact same spider (worker pool and
// delay queue) as the plain origin, including case-insensitive hosts.
func TestDefaultPortEquivalence(t *testing.T) {
	tc := newTestCrawler()

	plain, err := tc.getSpider(mustParseURL(t, "http://example.com/"))
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := tc.getSpider(mustParseURL(t, "http://example.com:80/"))
	if err != nil {
		t.Fatal(err)
	}
	upper, err := tc.getSpider(mustParseURL(t, "http://EXAMPLE.COM/"))
	if err != nil {
		t.Fatal(err)
	}
	if plain != explicit || plain != upper {
		t.Fatal("implicit port, explicit :80 and upper-case host must share one spider")
	}

	httpsPlain, err := tc.getSpider(mustParseURL(t, "https://example.com/"))
	if err != nil {
		t.Fatal(err)
	}
	httpsExplicit, err := tc.getSpider(mustParseURL(t, "https://example.com:443/"))
	if err != nil {
		t.Fatal(err)
	}
	if httpsPlain != httpsExplicit {
		t.Fatal("https implicit port and explicit :443 must share one spider")
	}
	if httpsPlain == plain {
		t.Fatal("http and https origins must not share a spider")
	}

	nonDefault, err := tc.getSpider(mustParseURL(t, "http://example.com:8080/"))
	if err != nil {
		t.Fatal(err)
	}
	if nonDefault == plain {
		t.Fatal("a non-default port must get its own spider")
	}
}

// TestIPv6SpiderIsolation verifies bracketed IPv6 hosts parse into a
// stable key instead of collapsing onto an empty host, and that
// different IPv6 origins get separate spiders.
func TestIPv6SpiderIsolation(t *testing.T) {
	tc := newTestCrawler()

	s1, err := tc.getSpider(mustParseURL(t, "http://[2001:db8::1]/"))
	if err != nil {
		t.Fatal(err)
	}
	s1Port, err := tc.getSpider(mustParseURL(t, "http://[2001:db8::1]:80/"))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := tc.getSpider(mustParseURL(t, "http://[2001:db8::2]/"))
	if err != nil {
		t.Fatal(err)
	}
	s3, err := tc.getSpider(mustParseURL(t, "http://[2001:db8::1]:8080/"))
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s1Port {
		t.Error("IPv6 host and the same host with explicit :80 must share a spider")
	}
	if s1 == s2 || s1 == s3 {
		t.Error("distinct IPv6 origins must have distinct spiders")
	}
}

// TestSpiderIdleReclamation verifies an idle spider is removed from the
// registry and a later request starts a fresh replacement rather than
// sending into a stopped spider.
func TestSpiderIdleReclamation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	tc := newTestCrawler()
	tc.spiderIdleTimeout = 30 * time.Millisecond
	items := make(chan Item, 8)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	if err := tc.EnqueueURL(ts.URL); err != nil {
		t.Fatal(err)
	}
	select {
	case <-items:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not complete")
	}

	s, err := tc.getSpider(mustParseURL(t, ts.URL))
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the short idle deadline to drain and unregister the
	// spider without touching its running fields.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tc.spiderMu.Lock()
		_, present := tc.spider[s.key]
		tc.spiderMu.Unlock()
		if !present {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	tc.spiderMu.Lock()
	_, present := tc.spider[s.key]
	tc.spiderMu.Unlock()
	if present {
		t.Fatal("idle spider was not reclaimed")
	}
	if s.alive() {
		t.Fatal("reclaimed spider still reports alive")
	}

	// A new request must create a replacement and complete successfully.
	if err := tc.EnqueueURL(ts.URL); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-items:
		if v != "ok" {
			t.Errorf("item = %v, want ok", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request after idle reclamation did not complete")
	}
	tc.spiderMu.Lock()
	n := len(tc.spider)
	tc.spiderMu.Unlock()
	if n != 1 {
		t.Errorf("expected exactly one replacement spider, got %d", n)
	}
}

// TestRepeatedStartNoDuplicateSpider hammers getSpider concurrently and
// sequentially to prove the registry never holds duplicate workers for
// one origin.
func TestRepeatedStartNoDuplicateSpider(t *testing.T) {
	tc := newTestCrawler()
	raw := "http://repeat.example.com/"

	first, err := tc.getSpider(mustParseURL(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s, err := tc.getSpider(mustParseURL(t, raw))
		if err != nil {
			t.Fatal(err)
		}
		if s != first {
			t.Fatalf("sequential start #%d created a duplicate spider", i)
		}
	}

	const workers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				s, err := tc.getSpider(mustParseURL(t, raw))
				if err != nil {
					errCh <- err
					return
				}
				if s != first {
					errCh <- fmt.Errorf("concurrent start created a duplicate spider")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	tc.spiderMu.Lock()
	n := len(tc.spider)
	tc.spiderMu.Unlock()
	if n != 1 {
		t.Errorf("expected one spider entry, got %d", n)
	}
}

// TestRequestFailureKeepsSpiderHealthy drives requests at a closed
// server: failures must be logged, not crash the spider or duplicate
// its workers, and a later request to a healthy server still works.
func TestRequestFailureKeepsSpiderHealthy(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	badURL := bad.URL
	bad.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer good.Close()

	tc := newTestCrawler()
	items := make(chan Item, 8)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	for i := 0; i < 3; i++ {
		if err := tc.EnqueueURL(badURL); err != nil {
			t.Fatal(err)
		}
	}
	if err := tc.EnqueueURL(good.URL); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-items:
		if v != "ok" {
			t.Errorf("item = %v, want ok", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("healthy site request did not complete after failures")
	}

	badKey, err := siteKey(mustParseURL(t, badURL))
	if err != nil {
		t.Fatal(err)
	}
	goodKey, err := siteKey(mustParseURL(t, good.URL))
	if err != nil {
		t.Fatal(err)
	}
	tc.spiderMu.Lock()
	n := len(tc.spider)
	badAlive := tc.spider[badKey] != nil
	goodAlive := tc.spider[goodKey] != nil
	tc.spiderMu.Unlock()
	if n != 2 || !badAlive || !goodAlive {
		t.Errorf("after failures, registry = %d entries (bad=%v good=%v), want 2 live spiders", n, badAlive, goodAlive)
	}
}

// TestCrawlerExitCancelsInFlight verifies closing Exit cancels an
// in-flight blocked request and leaves no spider registered afterwards.
func TestCrawlerExitCancelsInFlight(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer ts.Close()

	exit := make(chan struct{})
	tc := NewCrawler()
	tc.Exit = exit
	tc.DownloadDelay = time.Millisecond
	tc.Handle("*", VoidHandler())

	if err := tc.EnqueueURL(ts.URL); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request never reached the test server")
	}

	close(exit)
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight request was not canceled on Exit")
	}

	// The spider must unregister and stop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		tc.spiderMu.Lock()
		n := len(tc.spider)
		tc.spiderMu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("spider was not removed after Exit")
}

// TestSitesDoNotBlockEachOther makes a slow site and a fast site hit
// concurrently; the fast site must answer without waiting for the slow
// site's gate, proving per-site delay/worker isolation end to end.
func TestSitesDoNotBlockEachOther(t *testing.T) {
	slowGate := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-slowGate
		w.Write([]byte("slow"))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("fast"))
	}))
	defer fast.Close()

	tc := newTestCrawler()
	tc.DownloadDelay = 20 * time.Millisecond
	items := make(chan Item, 8)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	if err := tc.EnqueueURL(slow.URL); err != nil {
		t.Fatal(err)
	}
	// Give the slow request time to enter the gated handler.
	time.Sleep(50 * time.Millisecond)
	if err := tc.EnqueueURL(fast.URL); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-items:
		if v != "fast" {
			t.Errorf("first item = %v, want fast (slow site blocked the fast site)", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast site was blocked by the slow site")
	}
	close(slowGate)
	select {
	case v := <-items:
		if v != "slow" {
			t.Errorf("second item = %v, want slow", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow site never completed after releasing gate")
	}
}

// TestNoGoroutineLeakAfterReclamation repeatedly creates and idles out
// a spider and proves its workers exit instead of accumulating.
func TestNoGoroutineLeakAfterReclamation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	tc := newTestCrawler()
	tc.spiderIdleTimeout = 25 * time.Millisecond
	items := make(chan Item, 8)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	waitReclaimed := func(key string) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			tc.spiderMu.Lock()
			_, present := tc.spider[key]
			tc.spiderMu.Unlock()
			if !present {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("spider %q was not reclaimed", key)
	}

	// Warm up so the crawler's long-lived read/write loop workers exist
	// before taking the baseline count.
	if err := tc.EnqueueURL(ts.URL); err != nil {
		t.Fatal(err)
	}
	<-items
	key, err := siteKey(mustParseURL(t, ts.URL))
	if err != nil {
		t.Fatal(err)
	}
	waitReclaimed(key)
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	for round := 0; round < 3; round++ {
		if err := tc.EnqueueURL(ts.URL); err != nil {
			t.Fatal(err)
		}
		select {
		case <-items:
		case <-time.After(2 * time.Second):
			t.Fatal("request did not complete")
		}
		waitReclaimed(key)
		time.Sleep(50 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got != before {
		t.Fatalf("goroutine count changed from %d to %d after 3 reclaim cycles", before, got)
	}
}

// TestDownloadDelayIsPerSite verifies the download delay queue is owned
// per origin: two different sites each pay their own delay, but a
// request queued behind another site must not inherit that site's wait.
func TestDownloadDelayIsPerSite(t *testing.T) {
	tsA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("a"))
	}))
	defer tsA.Close()
	tsB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("b"))
	}))
	defer tsB.Close()

	tc := newTestCrawler()
	// A large delay: if the delay queue were shared across sites, the
	// second site's first request would be stalled by it.
	tc.DownloadDelay = 300 * time.Millisecond
	items := make(chan Item, 8)
	tc.UsePipeline(func(_ PipelineHandler) PipelineHandler {
		return PipelineHandlerFunc(func(v Item) { items <- v })
	})
	tc.Handle("*", HandlerFunc(func(c chan<- Item, resp *http.Response) {
		b, _ := ioutil.ReadAll(resp.Body)
		c <- string(b)
	}))

	start := time.Now()
	if err := tc.EnqueueURL(tsA.URL); err != nil {
		t.Fatal(err)
	}
	if err := tc.EnqueueURL(tsB.URL); err != nil {
		t.Fatal(err)
	}
	first := <-items
	second := <-items
	elapsed := time.Since(start)
	if first != "a" && first != "b" {
		t.Fatalf("unexpected item %v", first)
	}
	if second != "a" && second != "b" || first == second {
		t.Fatalf("expected one item per site, got %v and %v", first, second)
	}
	// Two independent 300ms delays run in parallel; a shared queue would
	// take roughly 600ms before both complete.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("both sites took %v; download delay appears shared across origins", elapsed)
	}
}

// TestFailureDoesNotRecreateSpider asserts a failed request leaves the
// same registered spider in place so its delay/worker state is not
// reset by transient errors.
func TestFailureDoesNotRecreateSpider(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	raw := ts.URL
	ts.Close()

	tc := newTestCrawler()
	tc.Handle("*", VoidHandler())

	s1, err := tc.getSpider(mustParseURL(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := tc.EnqueueURL(raw); err != nil {
			t.Fatal(err)
		}
	}
	// Give the failing exchanges time to finish; the registry entry must
	// remain the same pointer throughout.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		s2, err := tc.getSpider(mustParseURL(t, raw))
		if err != nil {
			t.Fatal(err)
		}
		if s2 != s1 {
			t.Fatal("failed request caused the spider to be recreated")
		}
		if !s1.alive() {
			t.Fatal("spider stopped after request failures")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
