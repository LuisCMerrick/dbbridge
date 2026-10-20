package main

import (
	"flag"
	"fmt"
	"io"
)

func printHelp(out io.Writer) {
	fmt.Fprint(out, `dbbridge - Peer-to-peer Database WS / HTTP-SSE Proxy

Usage:
  dbbridge <command> [options]

Commands:
  run             Run the peer node in the foreground or under a service manager
  gen-config      Generate a matched configuration pair (bidirectional by default)
  check-config    Validate a YAML configuration file and exit
  install         Install as Windows service or Linux systemd service
  uninstall       Stop and remove a service installed by DBBridge
  version         Print version information
  help            Print this help message

Common options (run, check-config, install):
  --config, -c        YAML configuration path (required)
  --log-level         debug, info, warn, error (default: info)
  --log-format        text, json (default: text)
  --env-file          Private service credential snapshot (normally automatic)

run options:
  --service-name      Service identity used for the Windows event log (default: dbbridge)

check-config options:
  --offline           Validate schema, defaults, references and listen addresses
                      without reading tokens, environment variables or TLS files

install options:
  --service-name      Service identity (default: dbbridge)
  --service-user      Linux account, or Windows LocalSystem/LocalService/NetworkService
  --exec-path         Executable written to the service definition (default: the
                      path dbbridge was invoked with; symbolic links are kept)
  --start             Also start after installation (default: false)
  --dry-run           Print the installation plan without changing the system

uninstall options:
  --service-name      Service identity (default: dbbridge)
  --dry-run           Check that the service exists and is managed by DBBridge

Credentials (token files, and the auth and token settings of the configuration
or credentials file) are re-read every auth.reload_interval and on SIGHUP
(Linux) or 'sc.exe control <name> paramchange' (Windows). Tokens taken from
environment variables only change with a restart.

Errors print a stable code, for example "Error [DBB-CFG-002]: ...".
Exit status: 0 success; 78 configuration or credential error (check-config,
run, install); 130 forced exit by a second interrupt during shutdown; 1 other
errors.

Examples:
  dbbridge gen-config
  dbbridge gen-config -o ./pair --a-peer-url https://b.example.com --b-peer-url https://a.example.com
  dbbridge check-config --config peer-b.yaml --offline
  dbbridge install --config peer-b.yaml --dry-run
  dbbridge install --config peer-b.yaml --service-user dbbridge --start
  dbbridge run --config peer-a.yaml
  dbbridge uninstall --service-name dbbridge

Use 'dbbridge gen-config --help' for generator options.
`)
}

// setCommandUsage prints the options of command for -h and --help.
func setCommandUsage(fs *flag.FlagSet, command string) {
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintf(out, "Usage: dbbridge %s [options]\n\nOptions:\n", command)
		fs.VisitAll(func(f *flag.Flag) {
			def := ""
			if f.DefValue != "" && f.DefValue != "false" {
				def = fmt.Sprintf(" (default %s)", f.DefValue)
			}
			fmt.Fprintf(out, "  -%s\n    \t%s%s\n", f.Name, f.Usage, def)
		})
	}
}
