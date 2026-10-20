package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func generateRandomHex(byteCount int) string {
	b := make([]byte, byteCount)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func parseBool(val interface{}, defaultVal bool) (bool, error) {
	if val == nil {
		return defaultVal, nil
	}
	switch v := val.(type) {
	case bool:
		return v, nil
	default:
		return false, fmt.Errorf("invalid boolean type: %T", val)
	}
}

func parseByteSize(val interface{}, defaultVal int64) (int64, error) {
	if val == nil {
		return defaultVal, nil
	}
	switch v := val.(type) {
	case int:
		if v < 0 {
			return 0, fmt.Errorf("byte size must be >= 0, got %d", v)
		}
		return int64(v), nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("byte size must be >= 0, got %d", v)
		}
		return v, nil
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return defaultVal, nil
		}
		re := regexp.MustCompile(`^(\d+)\s*([a-zA-Z]*)$`)
		matches := re.FindStringSubmatch(s)
		if len(matches) != 3 {
			return 0, fmt.Errorf("invalid byte size format: %s", s)
		}
		num, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, err
		}
		if num < 0 {
			return 0, fmt.Errorf("byte size must be >= 0: %s", s)
		}
		unit := strings.ToUpper(matches[2])
		switch unit {
		case "", "B":
			return num, nil
		case "K", "KB", "KIB":
			if num > math.MaxInt64/1024 {
				return 0, fmt.Errorf("byte size overflow: %s", s)
			}
			return num * 1024, nil
		case "M", "MB", "MIB":
			if num > math.MaxInt64/(1024*1024) {
				return 0, fmt.Errorf("byte size overflow: %s", s)
			}
			return num * 1024 * 1024, nil
		case "G", "GB", "GIB":
			if num > math.MaxInt64/(1024*1024*1024) {
				return 0, fmt.Errorf("byte size overflow: %s", s)
			}
			return num * 1024 * 1024 * 1024, nil
		default:
			return 0, fmt.Errorf("unknown byte unit: %s", unit)
		}
	default:
		return 0, fmt.Errorf("unexpected byte size type: %T", val)
	}
}

func parseDuration(val interface{}, defaultVal time.Duration) (time.Duration, error) {
	if val == nil {
		return defaultVal, nil
	}
	switch v := val.(type) {
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return defaultVal, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, err
		}
		if d < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %s", s)
		}
		return d, nil
	case int:
		if v < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %d", v)
		}
		if float64(v) > float64(math.MaxInt64/int64(time.Second)) {
			return 0, errors.New("duration overflow")
		}
		return time.Duration(v) * time.Second, nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %d", v)
		}
		if float64(v) > float64(math.MaxInt64/int64(time.Second)) {
			return 0, errors.New("duration overflow")
		}
		return time.Duration(v) * time.Second, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("duration must be a finite number: %v", v)
		}
		if v < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %v", v)
		}
		// Multiply in floating point so fractional seconds are kept; reject
		// values that would not fit in an int64 nanosecond count.
		ns := v * float64(time.Second)
		if ns >= math.MaxInt64 {
			return 0, errors.New("duration overflow")
		}
		return time.Duration(ns), nil
	default:
		return 0, fmt.Errorf("unexpected duration type: %T", val)
	}
}
