package fold

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sharathb5/sharathfold/store"
)

// DefaultDeliveryTimeout is used when Config.DeliveryTimeout is unset.
const DefaultDeliveryTimeout = 30 * time.Second

// HeaderFoldSignature is the Stripe-style versioned HMAC header:
//
//	Fold-Signature: t=<unix>,v1=<hex>
//
// The signed content is "<unix>.<raw body bytes>" — same shape as Stripe's
// Stripe-Signature scheme. Empty subscriber secret → header omitted.
const HeaderFoldSignature = "Fold-Signature"

// HTTPTransport POSTs the pre-encoded payload to the subscriber URL.
// It classifies the response; it does not schedule retries.
type HTTPTransport struct {
	client  *http.Client
	timeout time.Duration
	now     func() time.Time // tests may override
}

// NewHTTPTransport builds a transport with connection reuse.
// timeout <= 0 selects DefaultDeliveryTimeout. A nil client gets a
// shared-friendly default; if client is non-nil, timeout still bounds
// each Deliver via context.
func NewHTTPTransport(timeout time.Duration, client *http.Client) *HTTPTransport {
	if timeout <= 0 {
		timeout = DefaultDeliveryTimeout
	}
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}
	}
	return &HTTPTransport{client: client, timeout: timeout, now: time.Now}
}

func (t *HTTPTransport) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// SetClockForTest overrides the clock used for Fold-Signature timestamps.
func (t *HTTPTransport) SetClockForTest(now func() time.Time) {
	t.now = now
}

// Deliver POSTs d.Payload to d.URL. Payload must not be mutated.
// When d.Secret is non-empty, sets Fold-Signature over the existing payload
// bytes (no re-encoding).
func (t *HTTPTransport) Deliver(ctx context.Context, d store.Delivery) Result {
	if err := ctx.Err(); err != nil {
		return Result{Outcome: OutcomeRetryable, Err: err}
	}
	if d.URL == "" {
		return Result{Outcome: OutcomePermanent, Err: fmt.Errorf("empty subscriber URL")}
	}

	reqCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, d.URL, bytes.NewReader(d.Payload))
	if err != nil {
		return Result{Outcome: OutcomePermanent, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if d.EventType != "" {
		req.Header.Set("X-Fold-Event", d.EventType)
	}
	if d.EventID != "" {
		req.Header.Set("X-Fold-Event-Id", d.EventID)
	}
	if d.ID != "" {
		req.Header.Set("X-Fold-Delivery-Id", d.ID)
	}
	if len(d.Secret) > 0 {
		now := t.clock()
		req.Header.Set(HeaderFoldSignature, SignFold(d.Secret, now, d.Payload))
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return Result{Outcome: OutcomeRetryable, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))

	outcome := classifyStatus(resp.StatusCode)
	res := Result{Outcome: outcome, StatusCode: resp.StatusCode}
	if outcome != OutcomeSuccess {
		res.Err = fmt.Errorf("HTTP %s", resp.Status)
	}
	return res
}

// SignFold builds a Fold-Signature value: t=<unix>,v1=<hex hmac>.
// Signed bytes are strconv.FormatInt(unix,10) + "." + payload — not a
// re-serialization of the event.
func SignFold(secret []byte, at time.Time, payload []byte) string {
	ts := at.Unix()
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(strconv.FormatInt(ts, 10)))
	_, _ = mac.Write([]byte{'.'})
	_, _ = mac.Write(payload)
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

// VerifyFold checks a Fold-Signature header against secret and payload.
// tolerance is the max |now - t|; a non-positive tolerance skips the check.
func VerifyFold(secret []byte, header string, payload []byte, now time.Time, tolerance time.Duration) bool {
	ts, sig, ok := parseFoldSignature(header)
	if !ok || len(secret) == 0 || sig == "" {
		return false
	}
	if tolerance > 0 {
		delta := now.Unix() - ts
		if delta < 0 {
			delta = -delta
		}
		if time.Duration(delta)*time.Second > tolerance {
			return false
		}
	}
	expected := SignFold(secret, time.Unix(ts, 0).UTC(), payload)
	_, want, ok := parseFoldSignature(expected)
	if !ok {
		return false
	}
	return hmac.Equal([]byte(sig), []byte(want))
}

func parseFoldSignature(header string) (ts int64, v1 string, ok bool) {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "t=") {
			n, err := strconv.ParseInt(strings.TrimPrefix(part, "t="), 10, 64)
			if err != nil {
				return 0, "", false
			}
			ts = n
		}
		if strings.HasPrefix(part, "v1=") {
			v1 = strings.TrimPrefix(part, "v1=")
		}
	}
	return ts, v1, ts > 0 && v1 != ""
}

// classifyStatus maps an HTTP status to an Outcome.
// 2xx → success; 408/429/5xx → retryable; other 4xx → permanent.
func classifyStatus(code int) Outcome {
	switch {
	case code >= 200 && code < 300:
		return OutcomeSuccess
	case code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return OutcomeRetryable
	case code >= 500:
		return OutcomeRetryable
	default:
		return OutcomePermanent
	}
}
