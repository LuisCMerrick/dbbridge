package tunnel

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/transport"

	"github.com/gorilla/websocket"
)

func TestAuditReverseWSCloseInterruptsTLSBlackhole(t *testing.T) {
	stop := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		<-stop
	}))
	defer srv.Close()
	defer close(stop)
	d := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, HandshakeTimeout: time.Second}
	d.NetDialContext = func(ctx context.Context, n, a string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, n, a)
		if err != nil {
			return nil, err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetWriteBuffer(16 << 10)
		}
		return transport.NewIdleWriteConn(c), nil
	}
	c, _, err := d.Dial("wss"+strings.TrimPrefix(srv.URL, "https"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tc := newWSConn(c, nil)
	_ = tc.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	for i := 0; i < 10000; i++ {
		_, err = tc.Write(make([]byte, 64<<10))
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("fixture never stalled")
	}
	start := time.Now()
	_ = tc.Close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("reverse WSS close blocked on TLS close_notify after a carrier write timeout: %s", elapsed)
	}
}
