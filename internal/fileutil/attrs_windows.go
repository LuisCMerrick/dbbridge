package fileutil

import "os"

// PreserveAttributes is a no-op on Windows: replaced files keep the protected
// DACL the generator creates; grant service accounts access again with icacls.
func PreserveAttributes(os.FileInfo, string) error { return nil }
