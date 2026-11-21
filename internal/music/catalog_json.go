package music

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// Catalog DTOs have required, non-null fields throughout. Inspect presence before
// decoding so missing numeric fields cannot silently become valid zero values.
func decodeCatalogJSON(r io.Reader, target any) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := requireCatalogFields(data, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func requireCatalogFields(data json.RawMessage, typ reflect.Type) error {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("missing or null %s", typ)
	}
	switch typ.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			return err
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := field.Tag.Get("json")
			if err := requireCatalogFields(fields[name], field.Type); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if err := json.Unmarshal(data, &values); err != nil {
			return err
		}
		for i, value := range values {
			if err := requireCatalogFields(value, typ.Elem()); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	}
	return nil
}
