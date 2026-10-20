package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// DecodeJSON rejects missing required fields, duplicate/unknown keys, trailing values and invalid UTF-8.
func DecodeJSON(r io.Reader, limit int64, value any) error {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit || !utf8.Valid(b) {
		return errors.New("JSON limit or UTF-8 violation")
	}
	if err = checkSurrogates(b); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err = checkValue(d, 0); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	if err = validateShape(b, reflect.TypeOf(value), 0); err != nil {
		return err
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(value)
}

// checkSurrogates rejects \u escapes of UTF-16 surrogates that do not form a
// valid high+low pair; encoding/json would silently turn them into U+FFFD.
// A backslash only occurs inside JSON strings, so no string tracking is
// needed; malformed escapes are left for the decoder to reject.
func checkSurrogates(b []byte) error {
	hex4 := func(i int) (rune, bool) {
		if i+6 > len(b) || b[i] != '\\' || b[i+1] != 'u' {
			return 0, false
		}
		v, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 16)
		return rune(v), err == nil
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		r, ok := hex4(i)
		if !ok {
			i++ // skip the escaped character (e.g. \\ or \")
			continue
		}
		switch {
		case r >= 0xD800 && r <= 0xDBFF:
			lo, ok := hex4(i + 6)
			if !ok || lo < 0xDC00 || lo > 0xDFFF {
				return errors.New("JSON string has an unpaired UTF-16 surrogate escape")
			}
			i += 11
		case r >= 0xDC00 && r <= 0xDFFF:
			return errors.New("JSON string has an unpaired UTF-16 surrogate escape")
		default:
			i += 5
		}
	}
	return nil
}

func checkValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting limit")
	}
	tok, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			if !ok || keys[s] {
				return errors.New("duplicate JSON key")
			}
			keys[s] = true
			if err = checkValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err = checkValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}
func Offset(s string) (uint64, error) {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, errors.New("non-canonical offset")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("invalid offset")
		}
	}
	return strconv.ParseUint(s, 10, 64)
}

// ErrPeerProtocol is the comparable sentinel for a peer protocol violation
// (illegal data, wrong identity or binding). transport.ErrProtocol is this
// same value, so protocol-level validators can return errors that the
// lifecycle classifies as fatal protocol errors without the protocol
// package depending on transport.
var ErrPeerProtocol = errors.New("invalid peer protocol")

// ValidateBinding checks the Session, instance, Epoch, stream and transport
// binding of an ACK/POST/FIN response. A mismatch wraps ErrPeerProtocol
// (DBB_PROTOCOL_MISMATCH).
func ValidateBinding(sid, inst, ep, stream string, mode *TransportMode, wantSID, wantInst string, wantEpoch uint64, wantStream, wantMode string) error {
	if sid != wantSID || inst != wantInst || ep != strconv.FormatUint(wantEpoch, 10) || stream == "" || stream != wantStream || mode == nil || mode.Mode != wantMode {
		return fmt.Errorf("%w: %s: response binding mismatch", ErrPeerProtocol, ErrProtocolMismatch)
	}
	return nil
}

type jsonField struct {
	typ      reflect.Type
	required bool
}

var jsonFields sync.Map

func validateShape(raw json.RawMessage, typ reflect.Type, depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting limit")
	}
	if typ == nil {
		return errors.New("missing JSON destination")
	}
	// The outer decoder destination is a pointer, not a nullable protocol field.
	if depth == 0 {
		if typ.Kind() != reflect.Pointer {
			return errors.New("JSON destination must be a pointer")
		}
		typ = typ.Elem()
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		switch typ.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			return nil
		}
		return errors.New("null is not allowed for scalar or object fields")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		cached, ok := jsonFields.Load(typ)
		if !ok {
			fields := map[string]jsonField{}
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				name := strings.Split(f.Tag.Get("json"), ",")[0]
				// encoding/json flattens untagged embedded structs; this
				// validator does not, so refuse rather than mis-validate.
				if f.Anonymous && name == "" {
					return fmt.Errorf("protocol: embedded field %s in %s is not supported by DecodeJSON", f.Name, typ)
				}
				if !f.IsExported() {
					continue
				}
				if name == "-" {
					continue
				}
				if name == "" {
					name = f.Name
				}
				fields[name] = jsonField{f.Type, !strings.Contains(f.Tag.Get("json"), ",omitempty")}
			}
			cached, _ = jsonFields.LoadOrStore(typ, fields)
		}
		fields := cached.(map[string]jsonField)
		for name, field := range fields {
			if _, ok := object[name]; field.required && !ok {
				return fmt.Errorf("missing required JSON field %q", name)
			}
		}
		if typ == reflect.TypeFor[CreateSessionRequest]() {
			if err := rejectResponseOnlyLimits(object["limits"]); err != nil {
				return err
			}
		}
		if typ == reflect.TypeFor[WSTextControlMessage]() {
			var kind string
			if err := json.Unmarshal(object["type"], &kind); err != nil {
				return err
			}
			want := controlFields(kind)
			if want == nil {
				return fmt.Errorf("unknown control message type %q", kind)
			}
			for _, name := range want {
				if _, ok := object[name]; !ok {
					return fmt.Errorf("missing required %s field %q", kind, name)
				}
			}
			if _, ok := object["ack_requested"]; ok && kind != "ack_up" {
				return fmt.Errorf("field \"ack_requested\" not allowed for control type %q", kind)
			}
			for name := range object {
				if controlUnionFields[name] && !slices.Contains(want, name) {
					return fmt.Errorf("field %q not allowed for control type %q", name, kind)
				}
			}
		}
		for name, value := range object {
			field, ok := fields[name]
			if !ok {
				return fmt.Errorf("unknown JSON field %q", name)
			}
			if err := validateShape(value, field.typ, depth+1); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	case reflect.Slice, reflect.Array:
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := validateShape(value, typ.Elem(), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, value := range values {
			if err := validateShape(value, typ.Elem(), depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// responseOnlyLimits are SessionLimits fields that only an egress advertises
// in a create response; a create request never carries them.
var responseOnlyLimits = []string{"up_receipt_limit", "down_max_frames"}

// rejectResponseOnlyLimits refuses a create request whose limits object
// carries a response-only extension field: it would be an unknown field of
// the request schema.
func rejectResponseOnlyLimits(raw json.RawMessage) error {
	if raw == nil {
		return nil
	}
	var limits map[string]json.RawMessage
	if err := json.Unmarshal(raw, &limits); err != nil {
		return nil // reported by the shape check of the field itself
	}
	for _, name := range responseOnlyLimits {
		if _, ok := limits[name]; ok {
			return fmt.Errorf("limits: unknown JSON field %q", name)
		}
	}
	return nil
}
