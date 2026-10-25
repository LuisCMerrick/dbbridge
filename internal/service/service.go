package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"dbbridge/internal/config"
	"dbbridge/internal/protocol"
)

const marker = "Managed by DBBridge"

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
var envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type Options struct {
	Name, Config, Executable, LogLevel, LogFormat, User string
	Start, DryRun                                       bool
}
type Plan struct {
	Platform         string   `json:"platform"`
	Name             string   `json:"name"`
	Executable       string   `json:"executable"`
	Arguments        []string `json:"arguments"`
	WorkingDirectory string   `json:"working_directory"`
	Account          string   `json:"account"`
	Automatic        bool     `json:"automatic"`
	Start            bool     `json:"start"`
	UnitFile         string   `json:"unit_file,omitempty"`
	Unit             string   `json:"unit,omitempty"`
	EnvironmentFile  string   `json:"environment_file,omitempty"`
	EnvironmentNames []string `json:"environment_names,omitempty"`
	Warnings         []string `json:"warnings,omitempty"`
	// ShutdownTimeout is the longest graceful stop of the node
	// (config.MaxShutdownDuration: drain plus teardown). Service managers
	// get this plus a margin before they kill the process.
	ShutdownTimeout time.Duration `json:"-"`
	// MaxSessions sizes the file descriptor limit.
	MaxSessions int `json:"-"`
	environment map[string]string
	// readPaths are files the service account must be able to read: the
	// config, credential and token files, TLS material and CA bundles.
	readPaths []string
	// lowPort is set when a listener binds a privileged (<1024) port.
	lowPort bool
}

func ValidName(name string) bool {
	return namePattern.MatchString(name) && !strings.HasSuffix(name, ".service")
}

func Prepare(o Options) (Plan, error) {
	var p Plan
	if !ValidName(o.Name) {
		return p, errors.New("service-name must be 1-64 letters/digits/._-, without a .service suffix")
	}
	if o.LogLevel != "debug" && o.LogLevel != "info" && o.LogLevel != "warn" && o.LogLevel != "error" {
		return p, errors.New("invalid log-level")
	}
	if o.LogFormat != "text" && o.LogFormat != "json" {
		return p, errors.New("invalid log-format")
	}
	if o.Config == "" {
		return p, errors.New("--config is required")
	}
	configPath, err := filepath.Abs(o.Config)
	if err != nil {
		return p, err
	}
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return p, config.WithCode(config.CodeConfigInvalid, fmt.Errorf("config preflight: %w", err))
	}
	exe := o.Executable
	if exe == "" {
		exe, err = invokedExecutable()
		if err != nil {
			return p, err
		}
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		return p, err
	}
	// The path is kept as given (a symlink is not resolved), so a stable
	// link such as /usr/local/bin/dbbridge keeps working across upgrades
	// that re-point it.
	info, err := os.Stat(exe)
	if err != nil || !info.Mode().IsRegular() {
		return p, fmt.Errorf("service executable %s must be (or link to) a regular file", exe)
	}
	var execWarning string
	if resolved, e := filepath.EvalSymlinks(exe); e == nil && resolved != exe {
		execWarning = fmt.Sprintf("service executable %s is a symbolic link to %s; the service starts whatever the link points to when it starts", exe, resolved)
	}
	for _, path := range []string{configPath, exe, o.User} {
		if strings.ContainsAny(path, "\x00\r\n") {
			return p, errors.New("invalid control character in service paths/account")
		}
	}
	p = Plan{Platform: runtime.GOOS, Name: o.Name, Executable: exe, WorkingDirectory: filepath.Dir(configPath), Account: o.User, Automatic: true, Start: o.Start, ShutdownTimeout: cfg.MaxShutdownDuration(), MaxSessions: cfg.Limits.MaxSessions}
	p.Arguments = []string{"run", "--config", configPath, "--service-name", o.Name, "--log-level", o.LogLevel, "--log-format", o.LogFormat}
	if p.Platform == "windows" && p.Account == "" {
		p.Account = "LocalSystem"
	}
	if p.Platform == "windows" {
		if _, err = windowsAccount(p.Account); err != nil {
			return Plan{}, err
		}
	}
	if p.Platform == "linux" && p.Account == "" {
		p.Account = "root"
	}
	switch p.Account {
	case "root", "0":
		// The unit keeps an empty CapabilityBoundingSet, so root cannot
		// bypass file permissions: it reads only what owner/group/other
		// bits grant uid 0. The installer checks exactly that.
		p.Warnings = append(p.Warnings, "the service runs as root without capabilities (empty CapabilityBoundingSet): root cannot bypass file permissions, so the configuration, credentials and their directories must be readable by uid 0 through their mode bits; prefer a dedicated account with --service-user")
	case "LocalSystem":
		p.Warnings = append(p.Warnings, "the service runs with full privileges (LocalSystem); prefer LocalService with --service-user and grant it read access to the configuration and credentials")
	}
	if execWarning != "" {
		p.Warnings = append(p.Warnings, execWarning)
	}
	p.Warnings = append(p.Warnings, cfg.Warnings...)
	p.readPaths, err = serviceFiles(cfg, configPath)
	if err != nil {
		return Plan{}, config.WithCode(config.CodeCredential, err)
	}
	p.lowPort = usesPrivilegedPort(cfg)
	p.environment, err = credentialEnvironment(cfg)
	if err != nil {
		return Plan{}, config.WithCode(config.CodeEnvironment, err)
	}
	if len(p.environment) > 0 {
		p.EnvironmentFile = filepath.Join(p.WorkingDirectory, o.Name+".service-env.json")
		for name := range p.environment {
			p.EnvironmentNames = append(p.EnvironmentNames, name)
		}
		sort.Strings(p.EnvironmentNames)
		p.Arguments = append(p.Arguments, "--env-file", p.EnvironmentFile)
	}
	if p.Platform == "linux" {
		p.UnitFile = filepath.Join("/etc/systemd/system", o.Name+".service")
		p.Unit, err = systemdUnit(p)
		if err != nil {
			return Plan{}, err
		}
	}
	return p, nil
}

func Install(ctx context.Context, o Options) (Plan, error) {
	p, err := Prepare(o)
	if err != nil {
		return p, err
	}
	if o.DryRun {
		return p, nil
	}
	return p, install(ctx, p)
}

// credentialEnvironment snapshots every token variable the loader resolves
// at startup or on reload: those of the main configuration and of every
// entry of the credentials file, including outbound credentials that no peer
// references yet. A disabled identity's variables are included when set; they
// are not needed while it stays disabled.
func credentialEnvironment(c *config.Config) (map[string]string, error) {
	names, optional := map[string]bool{}, map[string]bool{}
	inbound := func(peers []config.InboundPeerConfig) {
		for _, p := range peers {
			for _, name := range p.TokenEnvs {
				if p.IsEnabled() {
					names[name] = true
				} else {
					optional[name] = true
				}
			}
		}
	}
	for _, p := range c.Peers {
		if p.TokenEnv != "" {
			names[p.TokenEnv] = true
		}
	}
	// A disabled reverse section is never resolved, so its variable is
	// not required.
	if c.Reverse.Enabled && c.Reverse.TokenEnv != "" {
		names[c.Reverse.TokenEnv] = true
	}
	inbound(c.Auth.InboundPeers)
	if c.Auth.CredentialsFile != "" {
		path := c.Auth.CredentialsFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(c.ConfigFileDir, path)
		}
		f, _, err := config.ReadCredentials(path)
		if err != nil {
			return nil, err
		}
		inbound(f.InboundPeers)
		for _, p := range f.OutboundPeers {
			if p.TokenEnv != "" {
				names[p.TokenEnv] = true
			}
		}
	}
	values := map[string]string{}
	for name := range optional {
		if !names[name] && os.Getenv(name) != "" {
			names[name] = true
		}
	}
	for name := range names {
		if !safeEnvName(name) {
			return nil, fmt.Errorf("unsupported credential environment name %q", name)
		}
		value := os.Getenv(name)
		if value == "" || strings.ContainsAny(value, "\x00\r\n") {
			return nil, fmt.Errorf("credential environment %s is missing or malformed", name)
		}
		values[name] = value
	}
	return values, nil
}

// invokedExecutable is the path dbbridge was started with, made absolute
// without resolving symbolic links, or os.Executable when that path cannot
// be determined.
func invokedExecutable() (string, error) {
	if arg := os.Args[0]; arg != "" {
		path := arg
		if !strings.ContainsAny(arg, `/\`) {
			path, _ = exec.LookPath(arg)
		}
		if path != "" {
			if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
				return filepath.Abs(path)
			}
		}
	}
	return os.Executable()
}

// serviceFiles lists the files the installed service must be able to read.
func serviceFiles(c *config.Config, configPath string) ([]string, error) {
	seen := map[string]bool{}
	var out []string
	add := func(path, dir string) {
		if path == "" {
			return
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		if !seen[path] {
			seen[path] = true
			out = append(out, path)
		}
	}
	add(configPath, "")
	dir := c.ConfigFileDir
	if c.HTTP.Enabled && c.HTTP.TLS.Enabled {
		add(c.HTTP.TLS.CertFile, dir)
		add(c.HTTP.TLS.KeyFile, dir)
	}
	for _, p := range c.Peers {
		add(p.TokenFile, dir)
		add(p.TLS.CAFile, dir)
	}
	if c.Reverse.Enabled {
		add(c.Reverse.TokenFile, dir)
		add(c.Reverse.TLS.CAFile, dir)
	}
	for _, p := range c.Auth.InboundPeers {
		// A disabled identity's token sources are never read, so its
		// files may already be gone.
		if !p.IsEnabled() {
			continue
		}
		for _, f := range p.TokenFiles {
			add(f, dir)
		}
	}
	if c.Auth.CredentialsFile != "" {
		path := c.Auth.CredentialsFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(dir, path)
		}
		add(path, "")
		_, refs, err := config.ReadCredentials(path)
		if err != nil {
			return nil, err
		}
		for _, ref := range refs {
			add(ref, "")
		}
	}
	return out, nil
}

func usesPrivilegedPort(c *config.Config) bool {
	addrs := []string{}
	if c.HTTP.Enabled {
		addrs = append(addrs, c.HTTP.Listen)
	}
	if c.Admin.Enabled {
		addrs = append(addrs, c.Admin.Listen)
	}
	for _, l := range c.Listeners {
		addrs = append(addrs, l.Listen)
	}
	for _, a := range addrs {
		if _, port, err := net.SplitHostPort(a); err == nil {
			if n, err := strconv.Atoi(port); err == nil && n > 0 && n < 1024 {
				return true
			}
		}
	}
	return false
}

func safeEnvName(name string) bool {
	if !envPattern.MatchString(name) {
		return false
	}
	upper := strings.ToUpper(name)
	// Names that change process, runtime, TLS trust or proxy behaviour must
	// never be injected through the credential snapshot.
	switch upper {
	case "HOME", "PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "USERPROFILE", "TEMP", "TMP", "TMPDIR", "NOTIFY_SOCKET", "LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", "WATCHDOG_USEC", "WATCHDOG_PID",
		"GODEBUG", "GOTRACEBACK", "GOMAXPROCS", "GOMEMLIMIT", "GOGC", "GOFIPS140", "GOCOVERDIR", "GORACE", "GOENV", "GOROOT",
		"HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "ALL_PROXY", "REQUEST_METHOD", "SSL_CERT_FILE", "SSL_CERT_DIR", "TZ", "ZONEINFO":
		return false
	}
	return !strings.HasPrefix(upper, "LD_") && !strings.HasPrefix(upper, "DYLD_")
}

// saveEnvironment writes the credential snapshot unless an identical one
// exists. created reports whether this call created the file, also when it
// returns an error; the caller removes a created file on failure.
func saveEnvironment(p Plan) (created bool, err error) {
	if p.EnvironmentFile == "" {
		return false, nil
	}
	b, err := json.MarshalIndent(p.environment, "", "  ")
	if err != nil {
		return false, err
	}
	b = append(b, '\n')
	if info, e := os.Lstat(p.EnvironmentFile); e == nil {
		if !info.Mode().IsRegular() {
			return false, errors.New("service environment must be a regular file")
		}
		existing, e := os.ReadFile(p.EnvironmentFile)
		if e != nil {
			return false, e
		}
		if !bytes.Equal(existing, b) {
			return false, errors.New("service environment snapshot already exists with different values; update it explicitly before reinstalling")
		}
		return false, validateEnvironmentAccess(p)
	} else if !os.IsNotExist(e) {
		return false, e
	}
	return writeEnvironmentFile(p, b)
}

// LoadEnvironment imports only explicitly referenced credential names from the
// service's private snapshot. No secrets appear in the installed command line.
func LoadEnvironment(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var values map[string]string
	if err = protocol.DecodeJSON(f, 1<<20, &values); err != nil {
		return fmt.Errorf("service environment: %w", err)
	}
	for name, value := range values {
		if !safeEnvName(name) || value == "" || strings.ContainsRune(value, 0) {
			return errors.New("invalid service environment entry")
		}
	}
	if err = checkEnvironmentFile(f); err != nil {
		return fmt.Errorf("service environment %s: %w", path, err)
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		// A value the service manager already provides (for example a
		// systemd EnvironmentFile) is newer than the install-time snapshot
		// and wins.
		if current, ok := os.LookupEnv(name); ok && current != "" {
			if current != values[name] {
				slog.Warn("service environment snapshot value ignored; the process environment already sets it", "name", name)
			}
			continue
		}
		if err = os.Setenv(name, values[name]); err != nil {
			return err
		}
	}
	return nil
}

// Runner returns only after cancellation has drained and stopped the node.
type Runner func(context.Context, func() error, io.Writer) error

func Uninstall(ctx context.Context, name string, dryRun bool) error {
	if !ValidName(name) {
		return errors.New("invalid service-name")
	}
	if dryRun {
		// Report what a real removal would refuse.
		return checkUninstall(ctx, name)
	}
	return uninstall(ctx, name)
}

// NotifyStopping tells the service manager that a graceful stop has begun
// (sd_notify STOPPING=1 under systemd; a no-op elsewhere).
func NotifyStopping() { _ = notifyStopping() }
