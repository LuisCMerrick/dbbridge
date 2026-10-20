package config

import (
	"errors"
	"io"
	"os"
)

// A stat check rejects large regular files without allocating their contents;
// the reader bound also covers growing files and non-regular inputs.
func readFileLimited(path string, limit int64, tooLarge string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > limit {
		return nil, errors.New(tooLarge)
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New(tooLarge)
	}
	return b, nil
}
