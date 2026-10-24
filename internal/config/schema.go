package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"reflect"
	"strings"
	"time"
)

// Check node types before decoding; yaml.v3 otherwise converts scalars to
// strings and treats explicit null as an omitted field.
func validateSchema(data []byte, target any) error {
	var doc yaml.Node
	if e := yaml.Unmarshal(data, &doc); e != nil {
		return e
	}
	// Each alias target is validated once per expected type (again only when
	// reached at a greater depth, which is bounded by the nesting limit), so
	// repeated aliases cannot make validation quadratic.
	type aliasKey struct {
		node *yaml.Node
		typ  reflect.Type
	}
	aliasDepth := map[aliasKey]int{}
	var walk func(*yaml.Node, reflect.Type, string, int) error
	walk = func(n *yaml.Node, typ reflect.Type, path string, depth int) error {
		if depth > 32 {
			return fmt.Errorf("%s: YAML nesting limit", path)
		}
		if n.Kind == yaml.DocumentNode {
			if len(n.Content) != 1 {
				return fmt.Errorf("configuration must contain one YAML object")
			}
			return walk(n.Content[0], typ, path, depth+1)
		}
		if n.Kind == yaml.AliasNode {
			key := aliasKey{n.Alias, typ}
			if seen, ok := aliasDepth[key]; ok && seen >= depth {
				return nil
			}
			aliasDepth[key] = depth
			return walk(n.Alias, typ, path, depth+1)
		}
		if n.Tag == "!!null" {
			return fmt.Errorf("%s: explicit null is not allowed; omit optional fields", path)
		}
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		invalid := func(want string) error { return fmt.Errorf("%s must be %s", path, want) }
		if typ == reflect.TypeFor[time.Duration]() {
			if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
				return invalid("a duration string")
			}
			return nil
		}
		switch typ.Kind() {
		case reflect.Struct:
			if n.Kind != yaml.MappingNode {
				return invalid("an object")
			}
			fields := map[string]reflect.Type{}
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				name := strings.Split(field.Tag.Get("yaml"), ",")[0]
				if field.IsExported() && name != "-" && name != "" {
					fields[name] = field.Type
				}
			}
			for i := 0; i < len(n.Content); i += 2 {
				key, val := n.Content[i], n.Content[i+1]
				if key.Tag != "!!str" {
					return invalid("an object with string keys")
				}
				field, ok := fields[key.Value]
				if !ok {
					return fmt.Errorf("field %q not found in %s", key.Value, path)
				}
				if err := walk(val, field, path+"."+key.Value, depth+1); err != nil {
					return err
				}
			}
		case reflect.Map:
			if n.Kind != yaml.MappingNode {
				return invalid("an object")
			}
			for i := 0; i < len(n.Content); i += 2 {
				if n.Content[i].Tag != "!!str" {
					return invalid("an object with string keys")
				}
				if err := walk(n.Content[i+1], typ.Elem(), path+"."+n.Content[i].Value, depth+1); err != nil {
					return err
				}
			}
		case reflect.Slice:
			if n.Kind != yaml.SequenceNode {
				return invalid("an array")
			}
			for i, child := range n.Content {
				if err := walk(child, typ.Elem(), fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
					return err
				}
			}
		case reflect.String:
			if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
				return invalid("a string")
			}
		case reflect.Bool:
			if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" {
				return invalid("a boolean")
			}
		case reflect.Int, reflect.Int64:
			if n.Kind != yaml.ScalarNode || n.Tag != "!!int" {
				return invalid("an integer")
			}
			switch path[strings.LastIndex(path, ".")+1:] {
			case "max_sessions", "max_endpoints_per_peer", "up_max_inflight_frames", "h2_pool_size", "receipt_limit_per_session", "healthy_threshold", "unhealthy_threshold", "max_concurrent_probes", "tunnels_per_endpoint":
				var value int64
				if n.Decode(&value) != nil || value <= 0 {
					return invalid("a positive integer")
				}
			}
		case reflect.Interface:
			if n.Kind != yaml.ScalarNode {
				return invalid("a scalar")
			}
			switch path[strings.LastIndex(path, ".")+1:] {
			case "enabled":
				if n.Tag != "!!bool" {
					return invalid("a boolean")
				}
			case "frame_size", "max_frame_size", "up_window_bytes", "ack_batch_bytes", "chunk_size", "buffer_per_direction", "global_buffer_limit", "suspended_buffer_limit", "codec_buffer_limit", "metadata_limit":
				if n.Tag != "!!int" && n.Tag != "!!str" {
					return invalid("an integer or byte-size string")
				}
			default: // Raw durations retain support for numeric seconds.
				if n.Tag != "!!str" && n.Tag != "!!int" && n.Tag != "!!float" {
					return invalid("a duration string or numeric seconds")
				}
			}
		default:
			return invalid("a supported configuration type")
		}
		return nil
	}
	return walk(&doc, reflect.TypeOf(target), "config", 0)
}
