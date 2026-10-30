// Package sdk implements the takt Go SDK. The public packages (workflow,
// activity, client, worker, encoded, testsuite and the root package) expose
// it through type aliases, the same way Cadence's client does.
package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// DataConverter turns values into payloads and back.
type DataConverter interface {
	// ToData encodes a list of values.
	ToData(value ...interface{}) ([]byte, error)
	// FromData decodes a payload into a list of value pointers.
	FromData(input []byte, valuePtr ...interface{}) error
}

// Value is an encoded value that can be decoded later.
type Value interface {
	// HasValue reports whether there is a value.
	HasValue() bool
	// Get decodes the value into valuePtr.
	Get(valuePtr interface{}) error
}

// Values is a list of encoded values.
type Values interface {
	HasValues() bool
	Get(valuePtr ...interface{}) error
}

type defaultDataConverter struct{}

var defaultConverter DataConverter = defaultDataConverter{}

// GetDefaultDataConverter returns the JSON converter used unless a worker or
// client is given another one. Like Cadence's, it writes one JSON document
// per value separated by newlines, and passes a lone []byte through as is.
func GetDefaultDataConverter() DataConverter { return defaultConverter }

func (defaultDataConverter) ToData(values ...interface{}) ([]byte, error) {
	if len(values) == 1 {
		if b, ok := values[0].([]byte); ok {
			return b, nil
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i, v := range values {
		if err := enc.Encode(v); err != nil {
			return nil, fmt.Errorf("encode argument %d (%T): %w", i, v, err)
		}
	}
	return buf.Bytes(), nil
}

func (defaultDataConverter) FromData(input []byte, valuePtrs ...interface{}) error {
	if len(valuePtrs) == 1 {
		if b, ok := valuePtrs[0].(*[]byte); ok {
			*b = append((*b)[:0], input...)
			return nil
		}
	}
	dec := json.NewDecoder(bytes.NewReader(input))
	for i, ptr := range valuePtrs {
		if err := dec.Decode(ptr); err != nil {
			return fmt.Errorf("decode argument %d (%T): %w", i, ptr, err)
		}
	}
	return nil
}

func orDefault(dc DataConverter) DataConverter {
	if dc == nil {
		return defaultConverter
	}
	return dc
}

// encodedValue implements Value over a payload.
type encodedValue struct {
	data []byte
	dc   DataConverter
}

func newEncodedValue(data []byte, dc DataConverter) Value {
	return &encodedValue{data: data, dc: orDefault(dc)}
}

func (v *encodedValue) HasValue() bool { return v != nil && v.data != nil }

func (v *encodedValue) Get(valuePtr interface{}) error {
	if !v.HasValue() {
		return ErrNoData
	}
	return v.dc.FromData(v.data, valuePtr)
}

// encodedValues implements Values over a payload.
type encodedValues struct {
	data []byte
	dc   DataConverter
}

func newEncodedValues(data []byte, dc DataConverter) Values {
	return &encodedValues{data: data, dc: orDefault(dc)}
}

func (v *encodedValues) HasValues() bool { return v != nil && v.data != nil }

func (v *encodedValues) Get(valuePtr ...interface{}) error {
	if !v.HasValues() {
		return ErrNoData
	}
	return v.dc.FromData(v.data, valuePtr...)
}

// ErrorDetailsValues holds error details that were never encoded, such as
// those passed to NewCustomError in the same process.
type ErrorDetailsValues []interface{}

func (v ErrorDetailsValues) HasValues() bool { return len(v) > 0 }

func (v ErrorDetailsValues) Get(valuePtr ...interface{}) error {
	if !v.HasValues() {
		return ErrNoData
	}
	if len(valuePtr) > len(v) {
		return ErrTooManyArg
	}
	for i, ptr := range valuePtr {
		if err := assignValue(v[i], ptr); err != nil {
			return err
		}
	}
	return nil
}

var (
	// ErrNoData is returned when decoding a value that is not there.
	ErrNoData = errors.New("no data available")
	// ErrTooManyArg is returned when asking for more details than exist.
	ErrTooManyArg = errors.New("too many arguments")
)

// assignValue copies from into the value toPtr points at. A []byte source
// is decoded with the default converter, which is how payloads travel
// through channels and futures.
func assignValue(from, toPtr interface{}) error {
	return decodeAndAssign(defaultConverter, from, toPtr)
}

func decodeAndAssign(dc DataConverter, from, toPtr interface{}) error {
	if toPtr == nil {
		return nil
	}
	rv := reflect.ValueOf(toPtr)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("value parameter provided is not a pointer")
	}
	if data, ok := from.([]byte); ok {
		return orDefault(dc).FromData(data, toPtr)
	}
	fv := reflect.ValueOf(from)
	if !fv.IsValid() {
		rv.Elem().Set(reflect.Zero(rv.Elem().Type()))
		return nil
	}
	target := rv.Elem()
	if !fv.Type().AssignableTo(target.Type()) {
		return fmt.Errorf("%s is not assignable to %s", fv.Type(), target.Type())
	}
	target.Set(fv)
	return nil
}

// encodeArgs encodes a call's arguments.
func encodeArgs(dc DataConverter, args []interface{}) ([]byte, error) {
	if len(args) == 0 {
		return nil, nil
	}
	return orDefault(dc).ToData(args...)
}

// decodeArgs decodes a payload into new values matching the parameter
// types of fn, skipping a leading context parameter.
func decodeArgs(dc DataConverter, fnType reflect.Type, data []byte, skip int) ([]reflect.Value, error) {
	n := fnType.NumIn() - skip
	if n <= 0 {
		return nil, nil
	}
	ptrs := make([]interface{}, n)
	for i := range n {
		ptrs[i] = reflect.New(fnType.In(i + skip)).Interface()
	}
	if len(data) > 0 {
		if err := orDefault(dc).FromData(data, ptrs...); err != nil {
			return nil, err
		}
	}
	out := make([]reflect.Value, n)
	for i, p := range ptrs {
		out[i] = reflect.ValueOf(p).Elem()
	}
	return out, nil
}

// newPtrLike returns a pointer to a new zero value of v's type.
func newPtrLike(v interface{}) interface{} {
	t := reflect.TypeOf(v)
	if t == nil {
		return nil
	}
	return reflect.New(t).Interface()
}

func derefPtr(p interface{}) interface{} { return reflect.ValueOf(p).Elem().Interface() }

// NewValue wraps data encoded by the default data converter.
func NewValue(data []byte) Value { return newEncodedValue(data, nil) }

// NewValues wraps data encoded by the default data converter.
func NewValues(data []byte) Values { return newEncodedValues(data, nil) }
