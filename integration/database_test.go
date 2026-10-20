//go:build database

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

var testedBinary string

// Always build the tested source. Never select an installed or stale binary.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "dbbridge-database-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot create test directory")
		os.Exit(1)
	}
	testedBinary = filepath.Join(dir, "dbbridge")
	if runtime.GOOS == "windows" {
		testedBinary += ".exe"
	}
	cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildvcs=false", "-o", testedBinary, "../cmd/dbbridge")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build tested source: %v\n%s", err, out)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func freeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func startPeer(t *testing.T, cfg, address, token string) *exec.Cmd {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, testedBinary, "run", "--config", path, "--log-level", "error")
	cmd.Env = append(os.Environ(), "DBB_DATABASE_TEST_TOKEN="+token)
	// Runtime logs stay private in the test directory, never print credentials.
	log, err := os.OpenFile(filepath.Join(t.TempDir(), "peer.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("child process did not exit")
		}
		log.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatal("peer exited before readiness; inspect private test log")
		default:
		}
		c, e := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if e == nil {
			c.Close()
			return cmd
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("peer readiness deadline exceeded")
	return nil
}

func TestRealDatabase(t *testing.T) {
	for _, engine := range []struct{ name, env string }{{"MySQL", "DBB_MYSQL_DSN"}, {"MariaDB", "DBB_MARIADB_DSN"}} {
		t.Run(engine.name, func(t *testing.T) {
			dsn := os.Getenv(engine.env)
			if dsn == "" {
				t.Skip("set " + engine.env + " for an isolated test database")
			}
			original, err := mysql.ParseDSN(dsn)
			if err != nil {
				t.Fatal("invalid test DSN (redacted)")
			}
			if original.Net != "tcp" || original.Addr == "" || original.DBName == "" {
				t.Fatal("test DSN must select TCP and an isolated database")
			}
			for _, mode := range []string{"ws", "http-sse"} {
				for _, resume := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/recovery=%t", mode, resume), func(t *testing.T) {
						baddr, aaddr := freeAddress(t), freeAddress(t)
						var secret [32]byte
						if _, err := rand.Read(secret[:]); err != nil {
							t.Fatal(err)
						}
						token := hex.EncodeToString(secret[:])
						b := fmt.Sprintf("node:\n  id: B\nhttp:\n  enabled: true\n  listen: %q\ntargets:\n  test-db:\n    network: tcp\n    address: %q\nauth:\n  inbound_peers:\n    - id: A\n      token_envs: [DBB_DATABASE_TEST_TOKEN]\n      allowed_targets: [test-db]\ntransport:\n  recovery:\n    enabled: %t\n", baddr, original.Addr, resume)
						startPeer(t, b, baddr, token)
						a := fmt.Sprintf("node:\n  id: A\npeers:\n  - id: B\n    expected_node_id: B\n    credential_ref: B\n    token_env: DBB_DATABASE_TEST_TOKEN\n    endpoints:\n      - id: primary\n        url: %q\nlisteners:\n  - id: test\n    listen: %q\n    peer: B\n    target: test-db\ntransport:\n  protocol: %s\n  recovery:\n    enabled: %t\n", "http://"+baddr, aaddr, mode, resume)
						startPeer(t, a, aaddr, token)
						proxied := *original
						proxied.Addr = aaddr
						proxied.Timeout = 5 * time.Second
						proxied.ReadTimeout = 10 * time.Second
						proxied.WriteTimeout = 10 * time.Second
						db, err := sql.Open("mysql", proxied.FormatDSN())
						if err != nil {
							t.Fatal("cannot open database (redacted)")
						}
						defer db.Close()
						db.SetMaxOpenConns(1)
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						conn, err := db.Conn(ctx)
						if err != nil {
							t.Fatal("database connection failed (redacted)")
						}
						defer conn.Close()
						var version string
						var before, after uint64
						if err = conn.QueryRowContext(ctx, "SELECT VERSION(), CONNECTION_ID()").Scan(&version, &before); err != nil {
							t.Fatal("identity query failed")
						}
						if engine.name == "MariaDB" && !strings.Contains(strings.ToLower(version), "mariadb") {
							t.Fatal("DBB_MARIADB_DSN does not refer to MariaDB")
						}
						if _, err = conn.ExecContext(ctx, "CREATE TEMPORARY TABLE dbbridge_patch_test (id INT PRIMARY KEY, payload LONGBLOB)"); err != nil {
							t.Fatal("cannot create session-local temporary table")
						}
						tx, err := conn.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						stmt, err := tx.PrepareContext(ctx, "INSERT INTO dbbridge_patch_test VALUES (?, ?)")
						if err != nil {
							tx.Rollback()
							t.Fatal(err)
						}
						payload := bytes.Repeat([]byte{0, 255, 13, 10, 128}, 110000)
						_, err = stmt.ExecContext(ctx, 1, payload)
						stmt.Close()
						if err != nil {
							tx.Rollback()
							t.Fatal("prepared binary BLOB insert failed")
						}
						if err = tx.Commit(); err != nil {
							t.Fatal(err)
						}
						var got []byte
						if err = conn.QueryRowContext(ctx, "SELECT payload FROM dbbridge_patch_test WHERE id=1").Scan(&got); err != nil || !bytes.Equal(got, payload) {
							t.Fatal("BLOB byte mismatch")
						}
						if err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&after); err != nil || before != after {
							t.Fatal("backend connection identity changed")
						}
						t.Logf("%s %s recovery=%t: transaction, prepared statement, temporary table, %d byte BLOB and connection identity verified", engine.name, mode, resume, len(payload))
					})
				}
			}
		})
	}
}
