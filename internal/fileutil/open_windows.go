package fileutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Go's Unix mode bits do not establish a Windows DACL. Create private files
// with a protected ACL from the outset, rather than tightening inherited access
// after writing a secret. The creator, Administrators and SYSTEM retain access.
func openNewFile(path string, mode fs.FileMode) (*os.File, error) {
	if mode.Perm()&0077 != 0 {
		return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	}
	return openPrivateFile(path, nil)
}

func openPrivateFile(path string, reader *windows.SID) (*os.File, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	sddl := fmt.Sprintf("D:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;%s)", user.User.Sid.String())
	if reader != nil {
		sddl += fmt.Sprintf("(A;;FR;;;%s)", reader.String())
	}
	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return nil, err
	}
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	h, err := windows.CreateFile(p, windows.GENERIC_WRITE, windows.FILE_SHARE_READ, &sa, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "create", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

// WriteNewForSID grants the selected service account read access at creation,
// while keeping the owner/Administrators/SYSTEM grants and protected DACL.
// created reports whether this call left a new file at path, as for WriteNew.
func WriteNewForSID(path string, data []byte, reader *windows.SID) (created bool, err error) {
	f, err := openPrivateFile(path, reader)
	if err != nil {
		return false, err
	}
	if err = writeNewFile(f, data); err != nil {
		return false, err
	}
	return true, nil
}

// ErrNullDACL reports a file without a DACL, which grants everyone access:
// fine for readability, wrong for a private credential file.
var ErrNullDACL = errors.New("private credential file has no restricting DACL")

var effectiveRights = windows.NewLazySystemDLL("advapi32.dll").NewProc("GetEffectiveRightsFromAclW")

// CheckReadAccessForSID is a conservative ACL preflight, not an impersonation
// check. It never widens the permissions of an existing credential file.
func CheckReadAccessForSID(path string, reader *windows.SID) error {
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	acl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if acl == nil {
		return ErrNullDACL
	}
	if err = effectiveRights.Find(); err != nil {
		return err
	}
	var pin runtime.Pinner
	pin.Pin(reader)
	defer pin.Unpin()
	trustee := windows.TRUSTEE{TrusteeForm: windows.TRUSTEE_IS_SID, TrusteeType: windows.TRUSTEE_IS_USER, TrusteeValue: windows.TrusteeValueFromSID(reader)}
	var rights windows.ACCESS_MASK
	code, _, _ := effectiveRights.Call(uintptr(unsafe.Pointer(acl)), uintptr(unsafe.Pointer(&trustee)), uintptr(unsafe.Pointer(&rights)))
	runtime.KeepAlive(sd)
	if code != 0 {
		return syscall.Errno(code)
	}
	if uint32(rights)&(windows.GENERIC_ALL|windows.GENERIC_READ) != 0 || uint32(rights)&windows.FILE_GENERIC_READ == windows.FILE_GENERIC_READ {
		return nil
	}
	return windows.ERROR_ACCESS_DENIED
}

// SyncDir is a no-op on Windows: directories cannot be opened for flushing
// with os.Open, and NTFS journals the rename metadata itself.
func SyncDir(string) error { return nil }
