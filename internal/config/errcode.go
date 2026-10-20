package config

import "errors"

// Stable error codes for configuration, startup and reload failures. They
// appear in CLI error output ("Error [DBB-CFG-002]: ..."), as the "code"
// attribute of log records and in the Windows event log. A code is never
// reused for a different meaning.
const (
	// CodeConfigRead: the configuration file cannot be read, is too large,
	// is not valid YAML or violates the schema (unknown keys, wrong types).
	CodeConfigRead = "DBB-CFG-001"
	// CodeConfigInvalid: a value or a cross reference is invalid.
	CodeConfigInvalid = "DBB-CFG-002"
	// CodeCredential: a credentials file, token file or token environment
	// variable is missing, unreadable or invalid, or the credential snapshot
	// violates a constraint (ACL target, quotas, credential_ref).
	CodeCredential = "DBB-CFG-003"
	// CodeTLS: a TLS certificate, key or CA bundle cannot be loaded.
	CodeTLS = "DBB-CFG-004"
	// CodeListen: listen addresses are invalid or conflict with each other.
	CodeListen = "DBB-CFG-005"
	// CodeEnvironment: the service credential snapshot (--env-file) cannot be
	// loaded or a credential environment variable needed for installation is
	// missing.
	CodeEnvironment = "DBB-CFG-006"
	// CodeStartFailed: the node failed to start (for example a listen
	// address is already in use).
	CodeStartFailed = "DBB-CFG-010"
	// CodeFatal: a background component failed while running.
	CodeFatal = "DBB-CFG-011"
	// CodeReloadRejected: a credential reload candidate was rejected; the
	// previous snapshot stays in effect.
	CodeReloadRejected = "DBB-CFG-020"
	// CodeServiceInstall: service installation or removal failed for a
	// reason other than the configuration.
	CodeServiceInstall = "DBB-CFG-030"
)

// CodedError attaches a stable code to an error. Error returns the wrapped
// message unchanged; callers that present the error add the code.
type CodedError struct {
	Code string
	Err  error
}

func (e *CodedError) Error() string { return e.Err.Error() }
func (e *CodedError) Unwrap() error { return e.Err }

// WithCode attaches code to err unless err already carries a code.
func WithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	var coded *CodedError
	if errors.As(err, &coded) {
		return err
	}
	return &CodedError{Code: code, Err: err}
}

// ErrorCode returns the code carried by err, or "" when it has none.
func ErrorCode(err error) string {
	var coded *CodedError
	if errors.As(err, &coded) {
		return coded.Code
	}
	return ""
}
