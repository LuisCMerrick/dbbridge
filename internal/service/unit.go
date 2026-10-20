package service

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var unixAccount = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_-]*\$?|[0-9]+)$`)

func unitQuote(s string, command bool) (string, error) {
	for _, r := range s {
		if r < 32 || r == 127 {
			return "", errors.New("control characters are not supported in unit values")
		}
	}
	s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`).Replace(s)
	if command {
		s = strings.ReplaceAll(s, "$", "$$")
	}
	return `"` + s + `"`, nil
}
func systemdUnit(p Plan) (string, error) {
	if !ValidName(p.Name) || !unixAccount.MatchString(p.Account) {
		return "", errors.New("invalid systemd service/account name")
	}
	exe, err := unitQuote(p.Executable, true)
	if err != nil {
		return "", err
	}
	args := []string{exe}
	for _, arg := range p.Arguments {
		q, e := unitQuote(arg, true)
		if e != nil {
			return "", e
		}
		args = append(args, q)
	}
	// WorkingDirectory is a single path value, not an ExecStart argument:
	// surrounding quotes would become part of the path on systemd.
	dir := strings.ReplaceAll(p.WorkingDirectory, "%", "%%")
	if strings.TrimSpace(dir) != dir || strings.HasSuffix(dir, `\`) {
		return "", errors.New("systemd working directory cannot end in whitespace or a backslash")
	}
	for _, r := range dir {
		if r < 32 || r == 127 {
			return "", errors.New("control character in working directory")
		}
	}
	// No capabilities are needed unless a listener binds a port below 1024.
	caps := ""
	if p.lowPort {
		caps = "CAP_NET_BIND_SERVICE"
	}
	// Keep configuration and credentials under a home or temporary
	// directory visible to the service.
	protectHome, privateTmp := "true", "true"
	paths := append([]string{p.Executable, p.WorkingDirectory, p.EnvironmentFile}, p.readPaths...)
	// Judge symlink targets too: a link under /etc pointing into /home is
	// unreadable with ProtectHome=true.
	for _, path := range paths {
		if resolved, err := filepath.EvalSymlinks(path); err == nil && resolved != path {
			paths = append(paths, resolved)
		}
	}
	for _, path := range paths {
		for _, prefix := range []string{"/home/", "/root/", "/run/user/"} {
			if strings.HasPrefix(path+"/", prefix) {
				protectHome = "read-only"
			}
		}
		for _, prefix := range []string{"/tmp/", "/var/tmp/"} {
			if strings.HasPrefix(path+"/", prefix) {
				privateTmp = "false"
			}
		}
	}
	stop := p.ShutdownTimeout + 15*time.Second
	if stop < 45*time.Second {
		stop = 45 * time.Second
	}
	// Each session holds a client or backend socket plus carrier
	// connections; size the descriptor limit from the session limit.
	nofile := max(8192, p.MaxSessions*4+1024)
	return fmt.Sprintf(`# %s
[Unit]
Description=DBBridge peer node (%s)
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=10

[Service]
Type=notify
NotifyAccess=main
User=%s
WorkingDirectory=%s
ExecStart=%s
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
RestartPreventExitStatus=%d
TimeoutStartSec=30s
TimeoutStopSec=%ds
KillSignal=SIGTERM
NoNewPrivileges=true
UMask=0077
LimitNOFILE=%d
StandardOutput=journal
StandardError=journal
CapabilityBoundingSet=%s
AmbientCapabilities=%s
ProtectSystem=strict
ProtectHome=%s
PrivateTmp=%s
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectControlGroups=true
ProtectClock=true
ProtectHostname=true
RestrictNamespaces=true
RestrictRealtime=true
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true
SystemCallArchitectures=native
SystemCallFilter=@system-service
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX

[Install]
WantedBy=multi-user.target
`, marker, p.Name, p.Account, dir, strings.Join(args, " "), ExitConfigError, int64(stop/time.Second), nofile, caps, caps, protectHome, privateTmp), nil
}

// ExitConfigError (EX_CONFIG) is the exit status for configuration and
// credential errors. The unit does not restart on it, since a restart cannot
// fix the configuration.
const ExitConfigError = 78
