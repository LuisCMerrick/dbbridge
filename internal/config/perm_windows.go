package config

// warnInsecurePermissions is a no-op on Windows: gen-config and the service
// installer create secret files with a protected DACL, and evaluating
// arbitrary ACLs here would need the account the service runs as.
func (c *Config) warnInsecurePermissions(string) {}
