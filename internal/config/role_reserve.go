package config

// ApplyIngressReserve reserves part of the shared Sessions and Metadata
// budgets for the local ingress listeners of a bidirectional node, so remote
// peers' egress sessions can never make every local TCP entry fail.
// It is the single reservation mechanism: egress admits with
// Resources.TryEgressSession/TryEgressMetadata, which leave the reserve free,
// while the local ingress keeps using Sessions.Try/Metadata.Try.
//
// The reserve is a quarter of the session limit (at least 1, none when the
// limit is 1) and an eighth of the metadata budget. Nodes without local
// listeners reserve nothing. The call is idempotent and keeps a reserve that
// was already set, so config and the egress server may both call it.
func (c *Config) ApplyIngressReserve() {
	r := c.Resources
	if r == nil || len(c.Listeners) == 0 {
		return
	}
	if s, m := r.IngressReserve(); s != 0 || m != 0 {
		return
	}
	var sessions int64
	if limit := r.Sessions.Limit(); limit > 1 {
		sessions = max(1, limit/4)
	}
	r.SetIngressReserve(sessions, r.Metadata.Limit()/8)
}
