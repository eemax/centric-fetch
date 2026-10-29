// Package pyjson parses JSON keeping object key order and writes it back exactly as
// Python's json.dumps(value, separators=(",", ":"), ensure_ascii=False) does, with or
// without sort_keys=True.
//
// That is the canonical form records are stored in, and its key-sorted variant is what gets
// hashed. Matching Python byte for byte lets other tools reproduce payloads and hashes.
package pyjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Value is nil, bool, string, Number, []Value or *Object.
type Value any

// Number is a JSON number's literal text.
type Number string

// Object is a JSON object in document order. A repeated key keeps its first position and
// its last value, like a Python dict.
type Object struct {
	Keys   []string
	Values []Value
	index  map[string]int
}

// Get returns the value of key and whether it is present.
func (o *Object) Get(key string) (Value, bool) {
	if i, ok := o.index[key]; ok {
		return o.Values[i], true
	}
	return nil, false
}

func (o *Object) set(key string, v Value) {
	if o.index == nil {
		o.index = map[string]int{}
	}
	if i, ok := o.index[key]; ok {
		o.Values[i] = v
		return
	}
	o.index[key] = len(o.Keys)
	o.Keys = append(o.Keys, key)
	o.Values = append(o.Values, v)
}

// Parse reads exactly one JSON value.
func Parse(data []byte) (Value, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("invalid JSON: data after the top-level value")
	}
	return v, nil
}

func parseValue(dec *json.Decoder) (Value, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '[':
			list := []Value{}
			for dec.More() {
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				list = append(list, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("invalid JSON: %w", err)
			}
			return list, nil
		case '{':
			obj := &Object{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("invalid JSON: %w", err)
				}
				key, _ := keyTok.(string)
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				obj.set(key, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("invalid JSON: %w", err)
			}
			return obj, nil
		}
		return nil, fmt.Errorf("invalid JSON: unexpected %v", t)
	case json.Number:
		return Number(t), nil
	default:
		return t, nil // nil, bool, string
	}
}

// Encode writes v like Python's json.dumps with separators=(",", ":"), ensure_ascii=False
// and, when sortKeys, sort_keys=True.
func Encode(v Value, sortKeys bool) []byte {
	var b bytes.Buffer
	encode(&b, v, sortKeys)
	return b.Bytes()
}

// EncodeTo is Encode into b, which it resets first: callers encoding many values reuse one
// buffer.
func EncodeTo(b *bytes.Buffer, v Value, sortKeys bool) {
	b.Reset()
	encode(b, v, sortKeys)
}

func encode(b *bytes.Buffer, v Value, sortKeys bool) {
	switch t := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		writeString(b, t)
	case Number:
		b.WriteString(pyNumber(string(t)))
	case []Value:
		b.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			encode(b, item, sortKeys)
		}
		b.WriteByte(']')
	case *Object:
		b.WriteByte('{')
		if !sortKeys {
			for i, k := range t.Keys {
				if i > 0 {
					b.WriteByte(',')
				}
				writeString(b, k)
				b.WriteByte(':')
				encode(b, t.Values[i], sortKeys)
			}
			b.WriteByte('}')
			return
		}
		order := make([]int, len(t.Keys))
		for i := range order {
			order[i] = i
		}
		// Byte order of UTF-8 is code point order, which is how Python sorts str.
		slices.SortFunc(order, func(i, j int) int { return strings.Compare(t.Keys[i], t.Keys[j]) })
		for n, i := range order {
			if n > 0 {
				b.WriteByte(',')
			}
			writeString(b, t.Keys[i])
			b.WriteByte(':')
			encode(b, t.Values[i], sortKeys)
		}
		b.WriteByte('}')
	default:
		panic(fmt.Sprintf("pyjson: unsupported value %T", v))
	}
}

// writeString escapes like Python's encode_basestring: quote, backslash and control
// characters only; everything else (including U+2028/U+2029 and DEL) is written raw.
func writeString(b *bytes.Buffer, s string) {
	b.WriteByte('"')
	// Runs of characters written as they are go out in one copy.
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			if c < utf8.RuneSelf {
				i++
				continue
			}
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				// Invalid UTF-8 (Go strings can hold it; Python's can't): as U+FFFD.
				b.WriteString(s[start:i])
				b.WriteRune(utf8.RuneError)
				i++
				start = i
				continue
			}
			i += size
			continue
		}
		b.WriteString(s[start:i])
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			const hex = "0123456789abcdef"
			b.WriteString(`\u00`)
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		}
		i++
		start = i
	}
	b.WriteString(s[start:])
	b.WriteByte('"')
}

// pyNumber renders a JSON number literal the way Python reads and writes it back: an
// integer literal stays an exact integer, anything with a fraction or exponent becomes a
// float and is written with repr().
func pyNumber(lit string) string {
	if !strings.ContainsAny(lit, ".eE") {
		neg := strings.HasPrefix(lit, "-")
		digits := strings.TrimLeft(strings.TrimPrefix(lit, "-"), "0")
		if digits == "" {
			return "0"
		}
		if neg {
			return "-" + digits
		}
		return digits
	}
	// Out of range becomes ±Inf or ±0, as in Python.
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil && !errors.Is(err, strconv.ErrRange) {
		return lit
	}
	return FloatRepr(f)
}

// FloatRepr formats f like Python's repr(float).
func FloatRepr(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case math.IsNaN(f):
		return "NaN"
	}
	// Shortest round-trip digits, e.g. "-1.2345e+06".
	e := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	if e[0] == '-' {
		sign, e = "-", e[1:]
	}
	mantissa, expText, _ := strings.Cut(e, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	exp, _ := strconv.Atoi(expText)
	if digits == "0" {
		return sign + "0.0"
	}
	decpt := exp + 1 // value = 0.<digits> * 10^decpt
	if decpt <= -4 || decpt > 16 {
		m := digits[:1]
		if len(digits) > 1 {
			m += "." + digits[1:]
		}
		x := decpt - 1
		xs := "+"
		if x < 0 {
			xs, x = "-", -x
		}
		return fmt.Sprintf("%s%se%s%02d", sign, m, xs, x)
	}
	switch {
	case decpt <= 0:
		return sign + "0." + strings.Repeat("0", -decpt) + digits
	case decpt >= len(digits):
		return sign + digits + strings.Repeat("0", decpt-len(digits)) + ".0"
	default:
		return sign + digits[:decpt] + "." + digits[decpt:]
	}
}

// Text renders a scalar the way Python's str() would for the values Centric sends as ids
// and timestamps. ok is false for null, objects and arrays.
func Text(v Value) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case Number:
		return pyNumber(string(t)), true
	case bool:
		if t {
			return "True", true
		}
		return "False", true
	}
	return "", false
}
