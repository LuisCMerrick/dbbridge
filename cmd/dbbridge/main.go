package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"dbbridge/internal/config"
	"dbbridge/internal/node"
	"dbbridge/internal/observability"
	"dbbridge/internal/protocol"
	"dbbridge/internal/service"
)

var (
	Version   = ""
	GitCommit = "unknown"
	BuildDate = "unknown"
)

func init() {
	Version, GitCommit, BuildDate = resolveBuildInfo(Version, GitCommit, BuildDate, debug.ReadBuildInfo)
}
func main() {
	if err := execute(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		printError(os.Stderr, err)
		os.Exit(exitCode(err))
	}
}

// printError writes err with its stable code when it has one.
func printError(w io.Writer, err error) {
	if code := config.ErrorCode(err); code != "" {
		fmt.Fprintf(w, "Error [%s]: %v\n", code, err)
		return
	}
	fmt.Fprintln(w, "Error:", err)
}

// isConfigurationCode reports whether code names a configuration or
// credential problem, which exits with status 78.
func isConfigurationCode(code string) bool {
	switch code {
	case config.CodeConfigRead, config.CodeConfigInvalid, config.CodeCredential, config.CodeTLS, config.CodeListen, config.CodeEnvironment:
		return true
	}
	return false
}
func execute(args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		printHelp(out)
		return errors.New("a command is required")
	}
	command, rest := args[0], args[1:]
	if command == "--instal" || command == "--install" {
		command = "install"
	} else if strings.HasPrefix(command, "-") && command != "-v" && command != "--version" && command != "-h" && command != "--help" {
		command = "run"
		rest = args
	}
	switch command {
	case "version", "-v", "--version":
		if len(rest) != 0 {
			return errors.New("version accepts no arguments")
		}
		fmt.Fprintf(out, "dbbridge version %s (commit: %s, built: %s, runtime: %s, protocol: %s)\n", Version, GitCommit, BuildDate, runtime.Version(), protocol.ProtocolVersionString)
		return nil
	case "help", "-h", "--help":
		printHelp(out)
		return nil
	case "gen-config":
		return generateCommand(rest, out, errOut)
	case "uninstall":
		fs := flag.NewFlagSet(command, flag.ContinueOnError)
		fs.SetOutput(errOut)
		name := fs.String("service-name", "dbbridge", "Service identity")
		dry := fs.Bool("dry-run", false, "Preview removal without changing the system")
		if err := parseFlags(fs, rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if err := service.Uninstall(context.Background(), *name, *dry); err != nil {
			return config.WithCode(config.CodeServiceInstall, err)
		}
		if *dry {
			fmt.Fprintf(out, "Would stop and remove service %s; configuration, credentials and the service credential snapshot are retained.\n", *name)
		} else {
			fmt.Fprintf(out, "Service %s removed; configuration and credentials retained.\n", *name)
		}
		return nil
	case "run", "check-config", "install":
		fs := flag.NewFlagSet(command, flag.ContinueOnError)
		fs.SetOutput(errOut)
		path := fs.String("config", "", "Path to YAML configuration")
		fs.StringVar(path, "c", "", "Configuration path (shorthand)")
		level := fs.String("log-level", "info", "debug, info, warn, error")
		format := fs.String("log-format", "text", "text or json")
		envFile := fs.String("env-file", "", "Private credential snapshot")
		name := fs.String("service-name", "dbbridge", "Service identity")
		account := fs.String("service-user", "", "Linux account; Windows built-in service account")
		start := fs.Bool("start", false, "Start after installation")
		dry := fs.Bool("dry-run", false, "Print service installation plan only")
		install := fs.Bool("install", false, "Install as a service (same as the install command)")
		fs.BoolVar(install, "instal", false, "Deprecated spelling of --install")
		offline := fs.Bool("offline", false, "check-config: do not read tokens, environment variables or TLS files")
		execPath := fs.String("exec-path", "", "Executable path written to the service definition (default: the path dbbridge was invoked with; symlinks are kept)")
		setCommandUsage(fs, command)
		if err := parseFlags(fs, rest); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if *path == "" {
			return errors.New("--config is required")
		}
		if *level != "debug" && *level != "info" && *level != "warn" && *level != "error" {
			return errors.New("invalid --log-level")
		}
		if *format != "text" && *format != "json" {
			return errors.New("invalid --log-format")
		}
		if *offline && (command != "check-config" || *install) {
			return errors.New("--offline is only supported by check-config")
		}
		if command != "run" || *install {
			if err := service.LoadEnvironment(*envFile); err != nil {
				return configError(config.WithCode(config.CodeEnvironment, err))
			}
		}
		if *execPath != "" && command != "install" && !*install {
			return errors.New("--exec-path requires installation")
		}
		if command == "install" || *install {
			if command == "check-config" {
				return errors.New("--instal cannot be combined with check-config")
			}
			if *install && command != "install" {
				fmt.Fprintln(errOut, "Note: 'dbbridge install --config ...' is the preferred form")
			}
			plan, err := service.Install(context.Background(), service.Options{Name: *name, Config: *path, Executable: *execPath, LogLevel: *level, LogFormat: *format, User: *account, Start: *start, DryRun: *dry})
			if err != nil {
				if isConfigurationCode(config.ErrorCode(err)) {
					return configError(err)
				}
				return config.WithCode(config.CodeServiceInstall, err)
			}
			for _, w := range plan.Warnings {
				fmt.Fprintln(errOut, "Warning:", w)
			}
			if *dry {
				encoder := json.NewEncoder(out)
				encoder.SetIndent("", "  ")
				return encoder.Encode(plan)
			}
			fmt.Fprintf(out, "Service %s installed for %s with automatic startup.\n", plan.Name, plan.Platform)
			if plan.EnvironmentFile != "" {
				fmt.Fprintf(out, "Credential environment snapshot: %s\n", plan.EnvironmentFile)
			}
			if *start {
				fmt.Fprintln(out, "Service started.")
			} else {
				fmt.Fprintln(out, "Start with systemctl start <name>.service (Linux) or sc.exe start <name> (Windows).")
			}
			return nil
		}
		if *dry || *start || *account != "" {
			return errors.New("--dry-run, --start and --service-user require installation")
		}
		if command == "check-config" {
			cfg, err := config.LoadConfigWithOptions(*path, config.LoadOptions{Offline: *offline})
			if err != nil {
				return configError(fmt.Errorf("config check: %w", err))
			}
			for _, w := range cfg.Warnings {
				fmt.Fprintln(errOut, "Warning:", w)
			}
			if *offline {
				fmt.Fprintf(out, "Config %s is structurally valid (offline: tokens, environment variables and TLS files were not checked). Node ID: %s\n", *path, cfg.Node.ID)
				return nil
			}
			fmt.Fprintf(out, "Config %s is valid. Node ID: %s\n", *path, cfg.Node.ID)
			return nil
		}
		if !service.ValidName(*name) {
			return errors.New("invalid --service-name")
		}
		absoluteConfig, err := filepath.Abs(*path)
		if err != nil {
			return err
		}
		return service.Run(*name, filepath.Dir(absoluteConfig), out, func(ctx context.Context, ready func() error, output io.Writer) error {
			observability.SetupLoggerOutput(*format, *level, output)
			if err := service.LoadEnvironment(*envFile); err != nil {
				err = config.WithCode(config.CodeEnvironment, err)
				slog.Error("failed to load service credential environment", "code", config.ErrorCode(err), "err", err)
				return configError(err)
			}
			cfg, err := config.LoadConfig(absoluteConfig)
			if err != nil {
				slog.Error("failed to load configuration", "code", config.ErrorCode(err), "err", err)
				return configError(err)
			}
			for _, w := range cfg.Warnings {
				slog.Warn("configuration warning", "warning", w)
			}
			n := node.NewNode(cfg)
			stopReload := service.OnReload(func() { _ = n.ReloadCredentials() })
			defer stopReload()
			return serve(ctx, n, ready, func() func() { return forceExitOnSecondSignal(errOut, os.Exit) })
		})
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional argument: %s", fs.Arg(0))
	}
	return nil
}

type stringList []string

func (v *stringList) String() string     { return strings.Join(*v, ",") }
func (v *stringList) Set(s string) error { *v = append(*v, s); return nil }
func generateCommand(args []string, out, errOut io.Writer) error {
	fs := flag.NewFlagSet("gen-config", flag.ContinueOnError)
	fs.SetOutput(errOut)
	o := config.DefaultGeneratePairOptions()
	fs.StringVar(&o.OutputDir, "output", o.OutputDir, "Directory for peer-a.yaml, peer-b.yaml, and shared.token")
	fs.StringVar(&o.OutputDir, "o", o.OutputDir, "Output directory (shorthand)")
	fs.StringVar(&o.Mode, "mode", o.Mode, "bidirectional, a-to-b, or b-to-a; always generates both nodes")
	fs.StringVar(&o.TokenFile, "token-file", "", "Shared token path relative to current directory; default: <output>/shared.token")
	fs.StringVar(&o.Protocol, "protocol", o.Protocol, "auto, ws, or http-sse")
	fs.BoolVar(&o.Recovery, "recovery", o.Recovery, "Enable session recovery on both nodes")
	fs.BoolVar(&o.AllowInsecureHTTP, "allow-insecure-http", false, "Allow non-loopback plaintext HTTP to the peer (writes peers[].allow_insecure_http)")
	fs.BoolVar(&o.Force, "force", false, "Replace both configs after validation; never replace the token")
	fs.IntVar(&o.MaxSessions, "max-sessions", o.MaxSessions, "Global and inbound peer session quota per node")
	for _, item := range []struct {
		prefix string
		node   *config.GeneratePairNodeOptions
	}{{"a", &o.A}, {"b", &o.B}} {
		prefix, node := item.prefix, item.node
		fs.StringVar(&node.NodeID, prefix+"-node-id", node.NodeID, "Node "+prefix+" identity")
		fs.StringVar(&node.Listen, prefix+"-listen", node.Listen, "Node "+prefix+" local TCP proxy listen address")
		fs.StringVar(&node.HTTPListen, prefix+"-http-listen", node.HTTPListen, "Node "+prefix+" inbound HTTP listen address")
		fs.StringVar(&node.AdminListen, prefix+"-admin-listen", node.AdminListen, "Node "+prefix+" loopback admin address for /healthz, /readyz and /metrics")
		fs.StringVar(&node.TargetID, prefix+"-target-id", node.TargetID, "Node "+prefix+" local backend target ID (referenced by the other node)")
		fs.StringVar(&node.TargetAddress, prefix+"-target-address", node.TargetAddress, "Node "+prefix+" local backend DB address")
		fs.StringVar(&node.TLSCert, prefix+"-tls-cert", "", "Node "+prefix+" existing inbound TLS certificate")
		fs.StringVar(&node.TLSKey, prefix+"-tls-key", "", "Node "+prefix+" existing inbound TLS private key")
		fs.Var((*stringList)(&node.PeerURLs), prefix+"-peer-url", "Remote origin dialed by node "+prefix+"; repeat for failover (default: other node's HTTP listener)")
		fs.Var((*stringList)(&node.AllowedCIDRs), prefix+"-allow-cidr", "Node "+prefix+" allowed backend network; repeat as needed (default: exact target IP)")
	}
	if err := parseFlags(fs, args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	files, err := config.GeneratePair(o)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Configuration pair generated and validated (mode: %s):\n  A: %s\n  B: %s\nShared token file: %s (deploy a copy with each node; its value is not printed)\n", o.Mode, files.ConfigA, files.ConfigB, files.TokenFile)
	return nil
}
