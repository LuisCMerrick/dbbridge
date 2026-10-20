package service

import (
	"bytes"
	"dbbridge/internal/fileutil"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestEnvironmentSnapshotSelectedWindowsAccountACL(t *testing.T) {
	for _, account := range []string{"LocalSystem", "LocalService", "NetworkService"} {
		t.Run(account, func(t *testing.T) {
			p := Plan{Account: account, EnvironmentFile: filepath.Join(t.TempDir(), "service.environment.json")}
			data := []byte(`{"TOKEN":"snapshot-value"}`)
			if _, err := writeEnvironmentFile(p, data); err != nil {
				t.Fatal(err)
			}
			if err := validateEnvironmentAccess(p); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(p.EnvironmentFile)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("snapshot contents differ")
			}
			sd, err := windows.GetNamedSecurityInfo(p.EnvironmentFile, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			if err != nil {
				t.Fatal(err)
			}
			control, _, err := sd.Control()
			if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
				t.Fatal("snapshot inherited its parent ACL")
			}
			if account == "LocalSystem" {
				return // SYSTEM already has the intentional full-control grant.
			}
			reader, _ := environmentReader(account)
			acl, _, err := sd.DACL()
			if err != nil || acl == nil {
				t.Fatal("missing DACL")
			}
			found := false
			for i := uint32(0); i < uint32(acl.AceCount); i++ {
				var ace *windows.ACCESS_ALLOWED_ACE
				if err := windows.GetAce(acl, i, &ace); err != nil {
					t.Fatal(err)
				}
				sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
				if windows.EqualSid(sid, reader) {
					found = true
					if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || uint32(ace.Mask) != windows.FILE_GENERIC_READ {
						t.Fatalf("service account grant is not read-only: %#x", ace.Mask)
					}
				}
			}
			if !found {
				t.Fatal("selected service SID missing from snapshot DACL")
			}
		})
	}
}

func TestEnvironmentExistingSnapshotACLIsNotWidened(t *testing.T) {
	p := Plan{Account: "LocalService", EnvironmentFile: filepath.Join(t.TempDir(), "existing.json"), environment: map[string]string{"TOKEN": "original"}}
	data := []byte("{\n  \"TOKEN\": \"original\"\n}\n")
	if _, err := fileutil.WriteNew(p.EnvironmentFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	sd, err := windows.GetNamedSecurityInfo(p.EnvironmentFile, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	before := sd.String()
	if created, err := saveEnvironment(p); err == nil || created {
		t.Fatal("unreadable existing snapshot was silently accepted or replaced")
	}
	after, err := windows.GetNamedSecurityInfo(p.EnvironmentFile, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil || after.String() != before {
		t.Fatal("existing snapshot ACL was widened")
	}
	got, err := os.ReadFile(p.EnvironmentFile)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("existing snapshot contents changed")
	}
}
