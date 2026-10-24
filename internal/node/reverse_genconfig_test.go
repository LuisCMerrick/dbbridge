package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbridge/internal/config"
)

// gen-config --reverse produces a pair that works as generated: the egress
// node (database side) opens no socket and dials the ingress node, whose
// listener reaches the database through the tunnel.
func TestGeneratedReversePairWorks(t *testing.T) {
	for _, mode := range []string{"a-to-b", "b-to-a"} {
		t.Run(mode, func(t *testing.T) {
			backend, greeting := mysqlLikeBackend(t)
			dir := t.TempDir()
			o := config.DefaultGeneratePairOptions()
			o.OutputDir, o.Mode, o.Reverse = dir, mode, true
			o.A.Listen, o.A.HTTPListen, o.A.AdminListen, o.A.TargetAddress = freeAddr(t), freeAddr(t), freeAddr(t), backend
			o.B.Listen, o.B.HTTPListen, o.B.AdminListen, o.B.TargetAddress = freeAddr(t), freeAddr(t), freeAddr(t), backend
			files, err := config.GeneratePair(o)
			if err != nil {
				t.Fatal(err)
			}
			pubPath, dbPath, listen := files.ConfigA, files.ConfigB, o.A.Listen
			if mode == "b-to-a" {
				pubPath, dbPath, listen = files.ConfigB, files.ConfigA, o.B.Listen
			}
			raw, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), "reverse:") || strings.Contains(string(raw), "\nhttp:") {
				t.Fatalf("database side config:\n%s", raw)
			}
			pubCfg, err := config.LoadConfig(pubPath)
			if err != nil {
				t.Fatal(err)
			}
			dbCfg, err := config.LoadConfig(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if len(pubCfg.Peers) != 1 || !pubCfg.Peers[0].IsReverse() || !pubCfg.HTTP.Enabled {
				t.Fatalf("public side: peers=%+v http=%v", pubCfg.Peers, pubCfg.HTTP.Enabled)
			}
			if !dbCfg.Reverse.Enabled || dbCfg.HTTP.Enabled || len(dbCfg.Peers) != 0 {
				t.Fatalf("database side: reverse=%v http=%v peers=%d", dbCfg.Reverse.Enabled, dbCfg.HTTP.Enabled, len(dbCfg.Peers))
			}
			pub, db := NewNode(pubCfg), NewNode(dbCfg)
			if err := pub.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer pub.Stop()
			if err := db.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer db.Stop()
			waitFor(t, 5*time.Second, "tunnels", func() bool { return db.tunnelCli.Live() == dbCfg.Reverse.TunnelsPerEndpoint })
			p := &reversePair{listen: listen, greeting: greeting}
			c := dialDB(t, p)
			defer c.Close()
			exchange(t, c, pattern(1<<20, 3))
		})
	}
}

func TestGenerateReverseRejectsBidirectional(t *testing.T) {
	o := config.DefaultGeneratePairOptions()
	o.OutputDir, o.Reverse = filepath.Join(t.TempDir(), "out"), true
	if _, err := config.GeneratePair(o); err == nil || !strings.Contains(err.Error(), "a-to-b or b-to-a") {
		t.Fatalf("err = %v", err)
	}
	o.Mode = "a-to-b"
	o.B.TLSCert, o.B.TLSKey = "x.pem", "x.key"
	if _, err := config.GeneratePair(o); err == nil || !strings.Contains(err.Error(), "no HTTP server") {
		t.Fatalf("err = %v", err)
	}
}
