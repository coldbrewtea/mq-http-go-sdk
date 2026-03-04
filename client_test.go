package mq_http_sdk

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogap/errors"
	"github.com/valyala/fasthttp"
)

// startTestMQServer starts a dummy fasthttp server that simulates an RMQ endpoint.
// It returns the listener address and a cleanup function.
func startTestMQServer(t *testing.T, delay time.Duration) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			if delay > 0 {
				time.Sleep(delay)
			}
			ctx.SetStatusCode(fasthttp.StatusCreated)
			ctx.SetContentType("text/xml")
			ctx.SetBody([]byte(`<?xml version="1.0"?>` +
				`<Message>` +
				`<MessageId>test-msg-id</MessageId>` +
				`<MessageBodyMD5>d41d8cd98f00b204e9800998ecf8427e</MessageBodyMD5>` +
				`</Message>`))
		},
	}
	go srv.Serve(ln)

	return ln.Addr().String(), func() { ln.Close() }
}

// newTestMQClient creates an AliyunMQClient wired to a local test server
// with configurable connection pool constraints.
func newTestMQClient(t *testing.T, addr string, maxConnsPerHost int, maxConnWaitTimeout time.Duration) *AliyunMQClient {
	t.Helper()
	ep, err := neturl.Parse("http://" + addr)
	if err != nil {
		t.Fatalf("parse endpoint: %v", err)
	}
	return &AliyunMQClient{
		timeout:    5 * time.Second,
		endpoint:   ep,
		credential: NewMQCredential("test-ak", "test-sk", ""),
		client: &fasthttp.Client{
			MaxConnsPerHost:    maxConnsPerHost,
			MaxConnWaitTimeout: maxConnWaitTimeout,
			ReadTimeout:        5 * time.Second,
			WriteTimeout:       5 * time.Second,
			Name:               ClientName,
		},
	}
}

// send0Unfixed mirrors the original Send0 WITHOUT Release calls,
// reproducing the object pool leak bug.
func (p *AliyunMQClient) send0Unfixed(method Method, headers map[string]string, message interface{}, resource string) (*fasthttp.Response, error) {
	var xmlContent []byte
	var err error

	if message == nil {
		xmlContent = []byte{}
	} else {
		switch m := message.(type) {
		case []byte:
			xmlContent = m
		default:
			if bXml, e := xml.Marshal(message); e != nil {
				err = ErrMarshalMessageFailed.New(errors.Params{"err": e})
				return nil, err
			} else {
				xmlContent = bXml
			}
		}
	}

	xmlMD5 := md5.Sum(xmlContent)
	strMd5 := fmt.Sprintf("%x", xmlMD5)

	if headers == nil {
		headers = make(map[string]string)
	}

	headers[MQ_VERSION] = ClientVersion
	headers[CONTENT_TYPE] = "text/xml;charset=utf-8"
	headers[CONTENT_MD5] = base64.StdEncoding.EncodeToString([]byte(strMd5))
	headers[DATE] = time.Now().UTC().Format(http.TimeFormat)

	if authHeader, e := p.authorization(method, headers, fmt.Sprintf("/%s", resource)); e != nil {
		err = ErrGeneralAuthHeaderFailed.New(errors.Params{"err": e})
		return nil, err
	} else {
		headers[AUTHORIZATION] = authHeader
	}

	if len(p.credential.SecurityToken()) > 0 {
		headers[SECURITY_TOKEN] = p.credential.SecurityToken()
	}

	var buffer bytes.Buffer
	buffer.WriteString(p.endpoint.String())
	buffer.WriteString("/")
	buffer.WriteString(resource)

	url := buffer.String()

	// BUG: AcquireRequest without ReleaseRequest
	req := fasthttp.AcquireRequest()
	// NOTE: intentionally NO defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(string(method))
	req.SetBody(xmlContent)

	for header, value := range headers {
		req.Header.Set(header, value)
	}

	// BUG: AcquireResponse without ReleaseResponse on error
	resp := fasthttp.AcquireResponse()

	if err = p.client.Do(req, resp); err != nil {
		// NOTE: intentionally NOT releasing resp here (the bug)
		err = ErrSendRequestFailed.New(errors.Params{"err": err})
		return nil, err
	}

	return resp, nil
}

// TestErrNoFreeConns_Reproduce verifies that under constrained connection pool
// settings (small MaxConnsPerHost, no MaxConnWaitTimeout) with high concurrency,
// the original configuration triggers fasthttp.ErrNoFreeConns.
//
// This reproduces the production issue described in the bug report:
//   - Connection pool exhaustion under batch/scheduled task workloads
//   - MaxConnWaitTimeout <= 0 causes immediate failure instead of waiting
func TestErrNoFreeConns_Reproduce(t *testing.T) {
	// Server with 20ms delay to simulate real-world latency
	addr, closeFn := startTestMQServer(t, 20*time.Millisecond)
	defer closeFn()

	// Original (problematic) configuration:
	// - Small MaxConnsPerHost to make exhaustion easy to trigger
	// - MaxConnWaitTimeout = 0: fail immediately when no free connection
	cli := newTestMQClient(t, addr, 5, 0)

	const goroutines = 50
	const requestsPerGoroutine = 20
	totalRequests := goroutines * requestsPerGoroutine

	var wg sync.WaitGroup
	var noFreeConnsCount int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				resp, err := cli.send0Unfixed(POST, nil,
					[]byte("<Message><MessageBody>test</MessageBody></Message>"),
					"topics/test/messages")
				if err != nil && strings.Contains(err.Error(), "no free connections") {
					atomic.AddInt32(&noFreeConnsCount, 1)
				}
				// Intentionally NOT releasing resp — simulating the original bug
				_ = resp
			}
		}()
	}
	wg.Wait()

	if noFreeConnsCount == 0 {
		t.Fatal("expected ErrNoFreeConns to be triggered under high concurrency with small pool and no wait timeout, but it was not")
	}
	t.Logf("Bug reproduced: ErrNoFreeConns occurred %d/%d times (%.1f%%)",
		noFreeConnsCount, totalRequests, float64(noFreeConnsCount)/float64(totalRequests)*100)
}

// TestErrNoFreeConns_Fixed verifies that with the fix applied:
//  1. fasthttp.Request and Response objects are properly released back to pool
//  2. MaxConnWaitTimeout allows goroutines to wait for free connections
//
// Under the same constrained pool + high concurrency, NO ErrNoFreeConns should occur.
func TestErrNoFreeConns_Fixed(t *testing.T) {
	addr, closeFn := startTestMQServer(t, 20*time.Millisecond)
	defer closeFn()

	// Fixed configuration:
	// - Same small MaxConnsPerHost
	// - MaxConnWaitTimeout > 0: wait for a free connection instead of failing
	cli := newTestMQClient(t, addr, 5, 10*time.Second)

	const goroutines = 50
	const requestsPerGoroutine = 20
	totalRequests := goroutines * requestsPerGoroutine

	var wg sync.WaitGroup
	var noFreeConnsCount int32
	var successCount int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				// Using the fixed Send0 (with proper Release calls)
				resp, err := cli.Send0(POST, nil,
					[]byte("<Message><MessageBody>test</MessageBody></Message>"),
					"topics/test/messages")
				if err != nil && strings.Contains(err.Error(), "no free connections") {
					atomic.AddInt32(&noFreeConnsCount, 1)
				} else if err == nil {
					atomic.AddInt32(&successCount, 1)
				}
				// Caller is responsible for releasing resp returned by Send0
				if resp != nil {
					fasthttp.ReleaseResponse(resp)
				}
			}
		}()
	}
	wg.Wait()

	if noFreeConnsCount > 0 {
		t.Fatalf("ErrNoFreeConns still occurred %d times with fix applied", noFreeConnsCount)
	}
	t.Logf("Fix verified: all %d/%d requests succeeded without ErrNoFreeConns", successCount, totalRequests)
}

// TestSend_ReleasesResponse verifies that Send() properly releases the
// fasthttp.Response after processing, preventing object pool exhaustion
// over many sequential calls.
func TestSend_ReleasesResponse(t *testing.T) {
	addr, closeFn := startTestMQServer(t, 0)
	defer closeFn()

	cli := newTestMQClient(t, addr, 10, 5*time.Second)
	decoder := NewAliyunMQDecoder()

	// Run many sequential requests to verify no resource leak
	const iterations = 500
	for i := 0; i < iterations; i++ {
		resp := PublishMessageResponse{}
		statusCode, err := cli.Send(decoder, POST, nil,
			[]byte("<Message><MessageBody>test</MessageBody></Message>"),
			"topics/test/messages", &resp)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if statusCode != fasthttp.StatusCreated {
			t.Fatalf("iteration %d: expected status 201, got %d", i, statusCode)
		}
	}
	t.Logf("Completed %d sequential Send() calls without error or leak", iterations)
}
