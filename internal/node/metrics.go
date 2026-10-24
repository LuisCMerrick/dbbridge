package node

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"

	"dbbridge/internal/observability"
	"dbbridge/internal/resource"
)

// writeMetrics renders the node's metric families. Every family is written
// once, in a fixed order, under its own HELP/TYPE header.
func (n *Node) writeMetrics(w io.Writer) {
	cfg := n.cfg
	if r := cfg.Resources; r != nil {
		atomic.StoreInt64(&observability.DefaultMetrics.BufferMemoryBytes, r.Payload.Used())
		fmt.Fprintf(w, "# HELP dbb_recovery_retained_bytes Bytes retained for session recovery\n# TYPE dbb_recovery_retained_bytes gauge\n")
		fmt.Fprintf(w, "dbb_recovery_retained_bytes %d\n", r.Suspended.Used())
		fmt.Fprintf(w, "# HELP dbb_recovery_retained_limit Limit on bytes retained for session recovery\n# TYPE dbb_recovery_retained_limit gauge\n")
		fmt.Fprintf(w, "dbb_recovery_retained_limit %d\n", r.Suspended.Limit())
		budgets := map[string]*resource.Budget{"payload": r.Payload, "codec": r.Codec, "metadata": r.Metadata, "control_metadata": r.ControlMetadata, "probe_metadata": r.ProbeMetadata, "sessions": r.Sessions, "probes": r.Probes, "recovery": r.Recovery, "cleanup": r.Cleanup, "creates": r.Creates, "inbound_connections": r.InboundConnections, "conn_metadata": r.ConnMetadata, "data_connections": r.DataConnections, "control_connections": r.ControlConnections, "data_requests": r.DataRequests, "control_requests": r.ControlRequests}
		for name, b := range budgets {
			if b == nil {
				delete(budgets, name)
			}
		}
		names := sortedKeys(budgets)
		fmt.Fprintf(w, "# HELP dbb_resource_in_use Resource budget units in use\n# TYPE dbb_resource_in_use gauge\n")
		for _, name := range names {
			fmt.Fprintf(w, "dbb_resource_in_use{resource=%q} %d\n", name, budgets[name].Used())
		}
		fmt.Fprintf(w, "# HELP dbb_resource_limit Resource budget limit\n# TYPE dbb_resource_limit gauge\n")
		for _, name := range names {
			fmt.Fprintf(w, "dbb_resource_limit{resource=%q} %d\n", name, budgets[name].Limit())
		}
		fmt.Fprintf(w, "# HELP dbb_resource_wait_seconds Time spent waiting for resource budgets\n# TYPE dbb_resource_wait_seconds summary\n")
		for _, name := range names {
			count, duration := budgets[name].WaitStats()
			fmt.Fprintf(w, "dbb_resource_wait_seconds_count{resource=%q} %d\n", name, count)
			fmt.Fprintf(w, "dbb_resource_wait_seconds_sum{resource=%q} %g\n", name, duration.Seconds())
		}
	}
	modes := map[string]int{}
	gauges := map[string]int64{}
	for _, sess := range n.registry.GetAllSessions() {
		modes[sess.GetTransportMode()]++
		for k, v := range sess.MetricsSnapshot() {
			gauges[k] += v
		}
	}
	for _, k := range sortedKeys(gauges) {
		fmt.Fprintf(w, "# HELP dbb_egress_%s Sum of %s over egress sessions\n# TYPE dbb_egress_%s gauge\ndbb_egress_%s %d\n", k, k, k, k, gauges[k])
	}
	fmt.Fprintf(w, "# HELP dbb_egress_carrier_sessions Egress sessions by carrier transport\n# TYPE dbb_egress_carrier_sessions gauge\n")
	for _, mode := range []string{"ws", "http-sse"} {
		fmt.Fprintf(w, "dbb_egress_carrier_sessions{transport=%q} %d\n", mode, modes[mode])
	}
	if n.hub != nil {
		n.hub.WriteMetrics(w)
	}
	if n.tunnelCli != nil {
		n.tunnelCli.WriteMetrics(w)
	}
	if n.ingressMgr != nil {
		n.ingressMgr.WriteMetrics(w)
		snapshot := n.ingressMgr.SnapshotMetrics()
		for _, name := range sortedKeys(snapshot) {
			fmt.Fprintf(w, "# HELP dbb_ingress_%s Ingress %s\n# TYPE dbb_ingress_%s %s\ndbb_ingress_%s %d\n", name, name, name, metricType(name), name, snapshot[name])
		}
	}
}

// metricType is "counter" for monotonically increasing *_total snapshot
// values and "gauge" otherwise.
func metricType(name string) string {
	if strings.HasSuffix(name, "_total") {
		return "counter"
	}
	return "gauge"
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
