package service

import (
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type notifyConnection interface {
	io.WriteCloser
	SetWriteDeadline(time.Time) error
}

func notifyReady() error {
	return sendReady(os.Getenv("NOTIFY_SOCKET"), dialNotify)
}

func notifyStopping() error {
	return sendState(os.Getenv("NOTIFY_SOCKET"), "STOPPING=1", dialNotify)
}

func dialNotify(address *net.UnixAddr) (notifyConnection, error) {
	return net.DialUnix("unixgram", nil, address)
}

func sendReady(path string, dial func(*net.UnixAddr) (notifyConnection, error)) error {
	return sendState(path, "READY=1", dial)
}

func sendState(path, state string, dial func(*net.UnixAddr) (notifyConnection, error)) error {
	if path == "" {
		return nil
	}
	if strings.HasPrefix(path, "@") {
		path = "\x00" + path[1:]
	}
	c, err := dial(&net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer c.Close()
	if err = c.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	_, err = c.Write([]byte(state))
	return err
}
