package transport

import (
	"bytes"
	"context"
	"dbbridge/internal/protocol"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

func (c *TripleTransportClient) request(ctx context.Context, client *http.Client, method, path string, body io.Reader, sid, token, inst string, ep uint64, header http.Header, out any, statuses ...int) error {
	if client.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, client.Timeout)
		defer cancel()
	}
	ctx, wait := withDialWait(ctx, c.tCfg.ControlRequestTimeout)
	if c.resources != nil {
		budget := c.resources.DataRequests
		if client == c.controlClient {
			budget = c.resources.ControlRequests
		}
		if err := budget.Take(ctx, 1); err != nil {
			return localBudgetError(ctx, fmt.Errorf("request budget: %w", err), nil)
		}
		defer budget.Put(1)
	}
	r, e := http.NewRequestWithContext(ctx, method, c.peerURL+path, body)
	if e != nil {
		return e
	}
	SetUserAgent(r.Header, c.tCfg)
	r.Header.Set("Content-Type", "application/json")
	if t := c.GetToken(); t != "" {
		r.Header.Set("Authorization", "Bearer "+t)
	}
	if token != "" {
		r.Header.Set(protocol.HeaderSessionToken, token)
	}
	if inst != "" {
		r.Header.Set(protocol.HeaderInstanceID, inst)
	}
	if sid != "" {
		if b, e := c.getBinding(sid, ep); e == nil {
			r.Header.Set(protocol.HeaderStreamID, b.id)
		}
	}
	for k, v := range header {
		r.Header[k] = v
	}
	resp, e := client.Do(r)
	if e != nil {
		return localBudgetError(ctx, e, wait)
	}
	defer func() {
		// Drain what is left (bounded) so HTTP/1.1 can reuse the connection.
		drainBody(resp.Body)
		resp.Body.Close()
	}()
	ok := false
	for _, v := range statuses {
		ok = ok || resp.StatusCode == v
	}
	if !ok {
		return ReadError(resp, inst)
	}
	if out != nil {
		if e = protocol.DecodeJSON(resp.Body, 32<<10, out); e != nil {
			return fmt.Errorf("%w: response schema: %v", ErrProtocol, e)
		}
	} else {
		_, e = io.Copy(io.Discard, io.LimitReader(resp.Body, 32<<10))
	}
	return e
}

// jsonBody encodes a request body. A marshal failure (a programming error for
// the fixed protocol structs) fails the request instead of sending an empty body.
func jsonBody(v any) io.Reader {
	b, err := json.Marshal(v)
	if err != nil {
		return errReader{fmt.Errorf("encode request body: %w", err)}
	}
	return bytes.NewReader(b)
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// sessionPath builds /v1/sessions/<sid><suffix>[?epoch=<ep>] with the
// peer-supplied session ID escaped as a single path segment.
func sessionPath(sid, suffix string, ep uint64, withEpoch bool) string {
	p := "/v1/sessions/" + url.PathEscape(sid) + suffix
	if withEpoch {
		p += "?" + url.Values{"epoch": []string{strconv.FormatUint(ep, 10)}}.Encode()
	}
	return p
}

// validSessionID limits peer-issued session IDs to RFC 3986 unreserved
// characters so they can never alter the request path or query.
func validSessionID(sid string) bool {
	if sid == "" || len(sid) > 128 {
		return false
	}
	for i := 0; i < len(sid); i++ {
		c := sid[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~') {
			return false
		}
	}
	return sid != "." && sid != ".."
}
