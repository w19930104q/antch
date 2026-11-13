package antch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Item is represents an item object.
type Item interface{}

// Crawler is core of web crawl server that provides crawl websites
// and calls pipeline to process for received data from their pages.
type Crawler struct {
	// CheckRedirect specifies the policy for handling redirects.
	CheckRedirect func(req *http.Request, via []*http.Request) error

	// MaxConcurrentRequests specifies the maximum number of concurrent
	// requests that will be performed.
	// Default is 16.
	MaxConcurrentRequests int

	// MaxConcurrentRequestsPerHost specifies the maximum number of
	// concurrent requests that will be performed to any single domain.
	// Default is 1.
	MaxConcurrentRequestsPerSite int

	// RequestTimeout specifies a time to wait before the HTTP Request times out.
	// Default is 30s.
	RequestTimeout time.Duration

	// DownloadDelay specifies delay time to wait before access same website.
	// Default is 0.25s.
	DownloadDelay time.Duration

	// MaxConcurrentItems specifies the maximum number of concurrent items
	// to process parallel in the pipeline.
	// Default is 32.
	MaxConcurrentItems int

	// UserAgent specifies the user-agent for the remote server.
	UserAgent string

	// ErrorLog specifies an optional logger for errors HTTP transports
	// and unexpected behavior from handlers.
	// If nil, logging goes to os.Stderr via the log package's
	// standard logger.
	ErrorLog Logger

	// Exit is an optional channel whose closure indicates that the Crawler
	// instance should be stop work and exit.
	Exit <-chan struct{}

	readCh  chan *http.Request
	writeCh chan Item

	client      *http.Client
	pipeHandler PipelineHandler
	mids        []Middleware
	pipes       []Pipeline

	// baseCtx is canceled once when Exit closes; every request context
	// is derived from it so in-flight HTTP exchanges stop on shutdown
	// without spawning a watcher goroutine per request.
	baseCtx    context.Context
	baseCancel context.CancelFunc

	// spiderIdleTimeout overrides the default idle lifetime of a site
	// spider when positive. It is used to keep reclamation deterministic
	// while the public configuration surface stays unchanged.
	spiderIdleTimeout time.Duration

	spider   map[string]*spider
	spiderMu sync.Mutex

	once sync.Once
	mu   sync.RWMutex
	m    map[string]muxEntry
}

// NewCrawler returns a new Crawler with default settings.
func NewCrawler() *Crawler {
	return &Crawler{
		UserAgent: "antch(github.com)",
		Exit:      make(chan struct{}),
	}
}

type muxEntry struct {
	pattern string
	h       Handler
}

// StartURLs starts crawling for the given URL list.
func (c *Crawler) StartURLs(URLs []string) {
	for _, URL := range URLs {
		c.EnqueueURL(URL)
	}
}

// Crawl puts an HTTP request into the working queue to crawling.
func (c *Crawler) Crawl(req *http.Request) error {
	if req == nil {
		return errors.New("req is nil")
	}
	return c.enqueue(req, 5*time.Second)
}

// EnqueueURL puts given URL into the backup URLs queue.
func (c *Crawler) EnqueueURL(URL string) error {
	if URL == "" {
		return errors.New("URL is nil")
	}
	req, err := http.NewRequest("GET", URL, nil)
	if err != nil {
		return err
	}
	return c.Crawl(req)
}

func (c *Crawler) enqueue(req *http.Request, timeout time.Duration) error {
	c.once.Do(c.init)
	select {
	case c.readCh <- req:
	case <-time.After(timeout):
		return errors.New("crawler: timeout, worker is busy")
	}
	return nil
}

// Handle registers the Handler for the given pattern.
// If pattern is "*" means will matches all requests if
// no any pattern matches.
func (c *Crawler) Handle(pattern string, handler Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pattern == "" {
		panic("crawler: invalid domain")
	}
	if handler == nil {
		panic("crawler: handler is nil")
	}
	if c.m == nil {
		c.m = make(map[string]muxEntry)
	}
	c.m[pattern] = muxEntry{pattern: pattern, h: handler}
}

// Handler returns a Handler for the give HTTP Response.
func (c *Crawler) Handler(res *http.Response) (h Handler, pattern string) {
	return c.handler(res.Request.Host, res.Request.URL.Path)
}

// UseMiddleware adds a Middleware to the crawler.
func (c *Crawler) UseMiddleware(m ...Middleware) *Crawler {
	c.mids = append(c.mids, m...)
	return c
}

// UsePipeline adds a Pipeline to the crawler.
func (c *Crawler) UsePipeline(p ...Pipeline) *Crawler {
	c.pipes = append(c.pipes, p...)
	return c
}

// UseCookies enables the cookies middleware to working.
func (c *Crawler) UseCookies() *Crawler {
	return c.UseMiddleware(CookiesMiddleware())
}

// UseCompression enables the HTTP compression middleware to
// supports gzip, deflate for HTTP Request/Response.
func (c *Crawler) UseCompression() *Crawler {
	return c.UseMiddleware(CompressionMiddleware())
}

// UseProxy enables proxy for each of HTTP requests.
func (c *Crawler) UseProxy(proxyURL *url.URL) *Crawler {
	return c.UseMiddleware(ProxyMiddleware(http.ProxyURL(proxyURL)))
}

// UseRobotstxt enables support robots.txt.
func (c *Crawler) UseRobotstxt() *Crawler {
	return c.UseMiddleware(RobotstxtMiddleware())
}

func (c *Crawler) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Output(2, fmt.Sprintf(format, args...))
	} else {
		log.Printf(format, args...)
	}
}

func (c *Crawler) transport() http.RoundTripper {
	ts := &http.Transport{
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   c.maxConcurrentRequestsPerSite() * 2,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DialContext:           proxyDialContext,
	}

	var stack HttpMessageHandler = HttpMessageHandlerFunc(func(req *http.Request) (*http.Response, error) {
		return ts.RoundTrip(req)
	})
	for i := len(c.mids) - 1; i >= 0; i-- {
		stack = c.mids[i](stack)
	}

	return roundTripperFunc(stack.Send)
}

func (c *Crawler) pipeline() PipelineHandler {
	var stack PipelineHandler = PipelineHandlerFunc(func(item Item) {})
	for i := len(c.pipes) - 1; i >= 0; i-- {
		stack = c.pipes[i](stack)
	}
	return stack
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (c *Crawler) pathMatch(pattern, path string) bool {
	n := len(pattern)
	if pattern[n-1] == '/' {
		pattern = pattern[:n-1]
	}
	return strings.Index(path, pattern) >= 0
}

func (c *Crawler) matchHandler(path string) (h Handler, pattern string) {
	var n = 0
	for k, v := range c.m {
		if !c.pathMatch(k, path) {
			continue
		}
		if h == nil || len(k) > n {
			n = len(k)
			h = v.h
			pattern = v.pattern
		}
	}
	return
}

func (c *Crawler) handler(host, path string) (h Handler, pattern string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	h, pattern = c.matchHandler(host + path)
	if h == nil {
		h, pattern = c.matchHandler(host)
	}
	if h == nil {
		h, pattern = c.matchHandler("*")
	}
	if h == nil {
		h, pattern = VoidHandler(), ""
	}
	return
}

func (c *Crawler) maxConcurrentRequestsPerSite() int {
	if v := c.MaxConcurrentRequestsPerSite; v > 0 {
		return v
	}
	return 1
}

func (c *Crawler) maxConcurrentRequests() int {
	if v := c.MaxConcurrentRequests; v > 0 {
		return v
	}
	return 16
}

func (c *Crawler) maxConcurrentItems() int {
	if v := c.MaxConcurrentItems; v > 0 {
		return v
	}
	return 32
}

func (c *Crawler) downloadDelay() time.Duration {
	if v := c.DownloadDelay; v > 0 {
		return v
	}
	return 250 * time.Millisecond // 0.25s
}

func (c *Crawler) requestTimeout() time.Duration {
	if v := c.RequestTimeout; v > 0 {
		return v
	}
	return 30 * time.Second
}

func (c *Crawler) init() {
	c.baseCtx, c.baseCancel = context.WithCancel(context.Background())
	go func() {
		select {
		case <-c.Exit:
			c.baseCancel()
		case <-c.baseCtx.Done():
		}
	}()

	c.client = &http.Client{
		Transport:     c.transport(),
		CheckRedirect: c.CheckRedirect,
		Timeout:       c.requestTimeout(),
	}

	c.pipeHandler = c.pipeline()
	c.readCh = make(chan *http.Request)
	c.writeCh = make(chan Item)
	go c.readLoop()
	go c.writeLoop()
}

func (c *Crawler) scanRequestWork(workCh chan chan *http.Request, closeCh chan int) {
	reqch := make(chan *http.Request)
	for {
		workCh <- reqch
		select {
		case req := <-reqch:
			c.handleRequest(req)
		case <-closeCh:
			return
		}
	}
}

// handleRequest routes a single request to its site spider and runs
// the HTTP exchange. Keeping this in its own method guarantees the
// per-request context is released when the iteration ends instead of
// being retained until the worker goroutine exits.
func (c *Crawler) handleRequest(req *http.Request) {
	resc := make(chan responseAndError, 1)
	spider, err := c.getSpider(req.URL)
	if err != nil {
		closeRequest(req)
		c.logf("crawler: resolve site failed: %v", err)
		return
	}

	if req.Header.Get("User-Agent") == "" && c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}

	// Derive the request context from the crawler-wide context so an
	// in-flight HTTP exchange is canceled when Exit closes; no
	// per-request goroutine is required. The per-request cancel releases
	// the context's resources once this exchange is done.
	ctx, cancel := context.WithCancel(c.baseCtx)
	defer cancel()
	req = req.WithContext(ctx)

	// A spider may be evicted and replaced between getSpider and this
	// send; retry with the current one so requests are never dropped and
	// never delivered to a stopped spider.
	for {
		select {
		case spider.reqch <- requestAndChan{req: req, ch: resc}:
		case <-c.Exit:
			closeRequest(req)
			return
		}
		if spider.alive() {
			break
		}
		spider, err = c.getSpider(req.URL)
		if err != nil {
			closeRequest(req)
			c.logf("crawler: resolve site failed: %v", err)
			return
		}
	}
	select {
	case re := <-resc:
		closeRequest(req)
		if re.err != nil {
			c.logf("crawler: send HTTP request got error: %v", re.err)
		} else {
			go func(res *http.Response) {
				defer closeResponse(res)
				defer func() {
					if r := recover(); r != nil {
						c.logf("crawler: Handler got panic error: %v", r)
					}
				}()
				h, _ := c.Handler(res)
				h.ServeSpider(c.writeCh, res)
			}(re.res)
		}
	case <-c.Exit:
		closeRequest(req)
	}
}

// readLoop reads HTTP crawl request from queue and to execute.
func (c *Crawler) readLoop() {
	closeCh := make(chan int)
	workCh := make(chan chan *http.Request, c.maxConcurrentRequests())

	for i := 0; i < c.maxConcurrentRequests(); i++ {
		go func() {
			c.scanRequestWork(workCh, closeCh)
		}()
	}

	for {
		select {
		case req := <-c.readCh:
			reqch := <-workCh
			reqch <- req
		case <-c.Exit:
			goto exit
		}
	}
exit:
	close(closeCh)
}

// writeLoop writes a received Item into the item pippeline.
func (c *Crawler) writeLoop() {
	closeCh := make(chan int)
	workCh := make(chan Item, c.maxConcurrentItems())

	for i := 0; i < c.maxConcurrentItems(); i++ {
		go func() {
			c.scanPipelineWork(workCh, closeCh)
		}()
	}
	for {
		select {
		case item := <-c.writeCh:
			workCh <- item
		case <-c.Exit:
			goto exit
		}
	}
exit:
	close(closeCh)
}

func (c *Crawler) scanPipelineWork(workCh chan Item, closeCh chan int) {
	for {
		select {
		case v := <-workCh:
			done := make(chan int)
			go func() {
				defer close(done)
				defer func() {
					if r := recover(); r != nil {
						c.logf("crawler: Handler got panic error: %v", r)
					}
				}()
				c.pipeHandler.ServePipeline(v)
			}()
			select {
			case <-done:
			case <-closeCh:
				return
			}
		case <-closeCh:
			return
		}
	}
}

// defaultSpiderIdleTimeout is the time a site spider stays alive
// without receiving any new request before it is reclaimed.
const defaultSpiderIdleTimeout = 120 * time.Second

// removeSpider unregisters the spider from the crawler. It only
// deletes the map entry when it still points at the same spider, so a
// freshly created replacement is never removed by an old spider that
// is shutting down concurrently.
func (c *Crawler) removeSpider(s *spider) {
	c.spiderMu.Lock()
	defer c.spiderMu.Unlock()
	if current, ok := c.spider[s.key]; ok && current == s {
		delete(c.spider, s.key)
	}
}

// getSpider returns the live spider for the given URL's site. Sites are
// isolated by normalized origin (scheme + lower-cased host + default
// port normalization), so each site gets its own concurrency budget and
// download delay queue.
func (c *Crawler) getSpider(u *url.URL) (*spider, error) {
	key, err := siteKey(u)
	if err != nil {
		return nil, err
	}

	c.spiderMu.Lock()
	defer c.spiderMu.Unlock()

	if c.spider == nil {
		c.spider = make(map[string]*spider)
	}

	if s, ok := c.spider[key]; ok {
		return s, nil
	}
	s := &spider{
		c:           c,
		reqch:       make(chan requestAndChan),
		key:         key,
		idleTimeout: c.spiderIdleTimeoutValue(),
		closeCh:     make(chan struct{}),
	}
	c.spider[key] = s
	go s.crawlLoop()
	return s, nil
}

func (c *Crawler) spiderIdleTimeoutValue() time.Duration {
	if c.spiderIdleTimeout > 0 {
		return c.spiderIdleTimeout
	}
	return defaultSpiderIdleTimeout
}

// siteKey builds the normalized origin key for a site:
// scheme://host:port with the host lower-cased, default ports for the
// scheme normalized and IPv6 hosts unwrapped from brackets. URLs
// without an explicit port therefore share the key of the equivalent
// default-port URL instead of collapsing onto an empty host.
func siteKey(u *url.URL) (string, error) {
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", errors.New("crawler: URL without host")
	}
	port := u.Port()
	if p := defaultPort(scheme); p != "" {
		if port == "" || port == p {
			port = p
		}
	}
	if strings.Contains(host, ":") {
		// IPv6 literal: keep the brackets in the rendered key.
		host = "[" + host + "]"
	}
	if port == "" {
		return scheme + "://" + host, nil
	}
	return scheme + "://" + host + ":" + port, nil
}

func defaultPort(scheme string) string {
	switch scheme {
	case "http":
		return "80"
	case "https":
		return "443"
	}
	return ""
}

type requestAndChan struct {
	req *http.Request
	ch  chan responseAndError
}

type responseAndError struct {
	res *http.Response
	err error
}

// spider is http spider for the single site. One spider owns the
// per-site worker pool, download delay queue and idle lifecycle for a
// single normalized origin.
type spider struct {
	c           *Crawler
	reqch       chan requestAndChan
	key         string
	idleTimeout time.Duration

	closeOnce sync.Once
	closeCh   chan struct{}

	// pending counts requests that have been accepted but whose
	// response has not been delivered yet. Eviction waits for it to
	// drain so in-flight work and its delay queue are never killed.
	pending sync.WaitGroup
}

// alive reports whether the spider is still running.
func (s *spider) alive() bool {
	select {
	case <-s.closeCh:
		return false
	default:
		return true
	}
}

func (s *spider) shutdown() {
	s.closeOnce.Do(func() { close(s.closeCh) })
}

func (s *spider) queueScanWorker(workCh chan chan requestAndChan) {
	rc := make(chan requestAndChan)
	for {
		select {
		case workCh <- rc:
		case <-s.closeCh:
			return
		}
		select {
		case c := <-rc:
			resp, err := s.c.client.Do(c.req)
			// c.ch is buffered, so this never blocks after eviction or
			// shutdown and the worker can always exit cleanly.
			c.ch <- responseAndError{resp, err}
		case <-s.closeCh:
			return
		}
	}
}

func (s *spider) crawlLoop() {
	idleTimer := time.NewTimer(s.idleTimeout)
	defer idleTimer.Stop()
	workCh := make(chan chan requestAndChan, s.c.maxConcurrentRequestsPerSite())

	for i := 0; i < s.c.maxConcurrentRequestsPerSite(); i++ {
		go s.queueScanWorker(workCh)
	}

	for {
		select {
		case rc := <-s.reqch:
			s.pending.Add(1)
			// Wait a moment before start fetching. The delay is per
			// site and stays interruptible by eviction or shutdown.
			if t := s.c.downloadDelay(); t > 0 {
				select {
				case <-time.After(t):
				case <-s.c.Exit:
					s.pending.Done()
					goto exit
				case <-s.closeCh:
					s.pending.Done()
					goto exit
				}
			}
			go func(rc requestAndChan) {
				defer s.pending.Done()
				var c chan requestAndChan
				select {
				case c = <-workCh:
				case <-s.closeCh:
					return
				}
				select {
				case c <- rc:
				case <-s.closeCh:
				}
			}(rc)
			// Push the idle deadline out; drain a stale tick first so
			// an old deadline cannot fire immediately afterwards.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(s.idleTimeout)
		case <-idleTimer.C:
			goto exit
		case <-s.c.Exit:
			goto exit
		}
	}

exit:
	// Unregister first so no new request is routed to this spider, then
	// wait for accepted work to finish before stopping the worker pool.
	s.c.removeSpider(s)
	s.pending.Wait()
	s.shutdown()
}

func closeRequest(r *http.Request) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}

func closeResponse(r *http.Response) {
	if r != nil && r.Body != nil {
		r.Body.Close()
	}
}
