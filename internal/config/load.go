package config

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LoadOptions changes how LoadConfigWithOptions validates a configuration.
type LoadOptions struct {
	// Offline validates schema, defaults, cross references and listen
	// addresses without reading tokens, environment variables, TLS
	// certificates, keys or CA bundles. An
	// offline-validated Config must not be used to run a node.
	Offline bool
}

// LoadConfig parses a YAML config file and resolves environment variables / files.
func LoadConfig(path string) (*Config, error) {
	return LoadConfigWithOptions(path, LoadOptions{})
}

// LoadConfigWithOptions is LoadConfig with explicit options.
func LoadConfigWithOptions(path string, opts LoadOptions) (*Config, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	data, err := readFileLimited(absPath, 4<<20, "config too large")
	if err != nil {
		return nil, WithCode(CodeConfigRead, fmt.Errorf("failed to read config file %s: %w", absPath, err))
	}

	if e := validateSchema(data, Config{}); e != nil {
		return nil, WithCode(CodeConfigRead, e)
	}
	cfg := &Config{
		ConfigFileDir: filepath.Dir(absPath),
		offline:       opts.Offline,
		path:          absPath,
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, WithCode(CodeConfigRead, fmt.Errorf("failed to unmarshal yaml config: %w", err))
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, WithCode(CodeConfigRead, errors.New("configuration must contain exactly one YAML document"))
	}

	if err := cfg.ApplyDefaultsAndValidate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadServerCertificate uses the same paths during check-config and startup.
func (c *Config) LoadServerCertificate() (tls.Certificate, error) {
	return tls.LoadX509KeyPair(c.resolvePath(c.HTTP.TLS.CertFile), c.resolvePath(c.HTTP.TLS.KeyFile))
}

// resolvePath makes a configuration-relative path absolute.
func (c *Config) resolvePath(path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(c.ConfigFileDir, path)
}
