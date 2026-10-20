package service

import (
	"dbbridge/internal/fileutil"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func environmentReader(account string) (*windows.SID, error) {
	name, err := windowsAccount(account)
	if err != nil {
		return nil, err
	}
	kind := windows.WinLocalSystemSid
	switch name {
	case `NT AUTHORITY\LocalService`:
		kind = windows.WinLocalServiceSid
	case `NT AUTHORITY\NetworkService`:
		kind = windows.WinNetworkServiceSid
	}
	return windows.CreateWellKnownSid(windows.WELL_KNOWN_SID_TYPE(kind))
}

func writeEnvironmentFile(p Plan, data []byte) (bool, error) {
	sid, err := environmentReader(p.Account)
	if err != nil {
		return false, err
	}
	return fileutil.WriteNewForSID(p.EnvironmentFile, data, sid)
}

func validateEnvironmentAccess(p Plan) error {
	sid, err := environmentReader(p.Account)
	if err != nil {
		return err
	}
	if err = fileutil.CheckReadAccessForSID(p.EnvironmentFile, sid); err != nil {
		return fmt.Errorf("cannot confirm service account read access to %s: %w; explicitly grant the selected service account file read access before installing", p.EnvironmentFile, err)
	}
	return nil
}

// checkEnvironmentFile: the snapshot's protected DACL is created by the
// installer; it is not re-evaluated at startup on Windows.
func checkEnvironmentFile(*os.File) error { return nil }
