package fold_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/store"
)

func TestClassifyHTTPStatus(t *testing.T) {
	cases := []struct {
		code int
		want fold.Outcome
	}{
		{200, fold.OutcomeSuccess},
		{201, fold.OutcomeSuccess},
		{204, fold.OutcomeSuccess},
		{408, fold.OutcomeRetryable},
		{429, fold.OutcomeRetryable},
		{500, fold.OutcomeRetryable},
		{502, fold.OutcomeRetryable},
		{503, fold.OutcomeRetryable},
		{400, fold.OutcomePermanent},
		{401, fold.OutcomePermanent},
		{404, fold.OutcomePermanent},
		{410, fold.OutcomePermanent},
		{422, fold.OutcomePermanent},
	}

	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.code)
		}))
		tr := fold.NewHTTPTransport(2*time.Second, srv.Client())
		res := tr.Deliver(context.Background(), store.Delivery{
			ID:      "dlg_1",
			URL:     srv.URL,
			Payload: []byte(`{"ok":true}`),
		})
		srv.Close()
		if res.Outcome != tc.want {
			t.Errorf("status %d: outcome=%v want %v", tc.code, res.Outcome, tc.want)
		}
		if res.StatusCode != tc.code {
			t.Errorf("status %d: StatusCode=%d", tc.code, res.StatusCode)
		}
	}
}

func TestHTTPTransportPostsPayload(t *testing.T) {
	var (
		method  string
		ctype   string
		body    []byte
		eventH  string
		eventID string
		delID   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		ctype = r.Header.Get("Content-Type")
		eventH = r.Header.Get("X-Fold-Event")
		eventID = r.Header.Get("X-Fold-Event-Id")
		delID = r.Header.Get("X-Fold-Delivery-Id")
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload := []byte(`{"id":"evt-1","type":"order.created","data":{}}`)
	tr := fold.NewHTTPTransport(2*time.Second, srv.Client())
	res := tr.Deliver(context.Background(), store.Delivery{
		ID:        "dlg_9",
		EventID:   "evt-1",
		EventType: "order.created",
		URL:       srv.URL,
		Payload:   payload,
	})
	if res.Outcome != fold.OutcomeSuccess {
		t.Fatalf("outcome=%v err=%v", res.Outcome, res.Err)
	}
	if method != http.MethodPost {
		t.Fatalf("method=%s", method)
	}
	if ctype != "application/json" {
		t.Fatalf("Content-Type=%q", ctype)
	}
	if string(body) != string(payload) {
		t.Fatalf("body=%s", body)
	}
	if eventH != "order.created" || eventID != "evt-1" || delID != "dlg_9" {
		t.Fatalf("headers event=%q id=%q delivery=%q", eventH, eventID, delID)
	}
}

func TestHTTPTransportNetworkErrorRetryable(t *testing.T) {
	tr := fold.NewHTTPTransport(50*time.Millisecond, nil)
	res := tr.Deliver(context.Background(), store.Delivery{
		URL:     "http://127.0.0.1:1/", // nothing listening
		Payload: []byte(`{}`),
	})
	if res.Outcome != fold.OutcomeRetryable {
		t.Fatalf("outcome=%v want retryable; err=%v", res.Outcome, res.Err)
	}
}

func TestHTTPTransportSignsWhenSecretSet(t *testing.T) {
	secret := []byte("whsec_test_secret")
	payload := []byte(`{"id":"evt-1","type":"t","data":null}`)
	fixed := time.Unix(1_700_000_000, 0).UTC()

	var gotHeader string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(fold.HeaderFoldSignature)
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tr := fold.NewHTTPTransport(2*time.Second, srv.Client())
	tr.SetClockForTest(func() time.Time { return fixed })

	res := tr.Deliver(context.Background(), store.Delivery{
		URL:     srv.URL,
		Payload: payload,
		Secret:  secret,
	})
	if res.Outcome != fold.OutcomeSuccess {
		t.Fatalf("outcome=%v err=%v", res.Outcome, res.Err)
	}
	if string(gotBody) != string(payload) {
		t.Fatalf("body mutated or re-encoded: %s", gotBody)
	}
	want := fold.SignFold(secret, fixed, payload)
	if gotHeader != want {
		t.Fatalf("Fold-Signature=%q want %q", gotHeader, want)
	}
	if !fold.VerifyFold(secret, gotHeader, payload, fixed, time.Minute) {
		t.Fatal("VerifyFold rejected a fresh signature")
	}
}

func TestHTTPTransportOmitsSignatureWithoutSecret(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(fold.HeaderFoldSignature)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tr := fold.NewHTTPTransport(2*time.Second, srv.Client())
	res := tr.Deliver(context.Background(), store.Delivery{
		URL:     srv.URL,
		Payload: []byte(`{}`),
	})
	if res.Outcome != fold.OutcomeSuccess {
		t.Fatal(res.Err)
	}
	if gotHeader != "" {
		t.Fatalf("expected no Fold-Signature, got %q", gotHeader)
	}
}

func TestSignFoldRejectsTamperAndReplay(t *testing.T) {
	secret := []byte("s")
	payload := []byte(`{"a":1}`)
	at := time.Unix(1_700_000_000, 0).UTC()
	hdr := fold.SignFold(secret, at, payload)
	if !fold.VerifyFold(secret, hdr, payload, at, time.Minute) {
		t.Fatal("valid signature failed verify")
	}
	if fold.VerifyFold(secret, hdr, []byte(`{"a":2}`), at, time.Minute) {
		t.Fatal("tampered payload accepted")
	}
	if fold.VerifyFold(secret, hdr, payload, at.Add(10*time.Minute), 5*time.Minute) {
		t.Fatal("stale timestamp accepted")
	}
}
