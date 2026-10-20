package service

import (
	"errors"
	"strings"
)

func windowsAccount(account string) (string, error) {
	switch strings.ToLower(account) {
	case "", "localsystem", `nt authority\system`:
		return "", nil
	case "localservice", `nt authority\localservice`:
		return `NT AUTHORITY\LocalService`, nil
	case "networkservice", `nt authority\networkservice`:
		return `NT AUTHORITY\NetworkService`, nil
	default:
		return "", errors.New("the Windows service-user must be LocalSystem, LocalService, or NetworkService")
	}
}
