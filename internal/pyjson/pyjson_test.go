package pyjson

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

// testdata/python_cases.json holds [input, json.dumps(...), json.dumps(..., sort_keys=True)]
// triples written by Python (testdata/gen_python_cases.py).
func TestMatchesPython(t *testing.T) {
	data, err := os.ReadFile("testdata/python_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases [][3]string
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		v, err := Parse([]byte(c[0]))
		if err != nil {
			t.Fatalf("%s: %v", c[0], err)
		}
		if got := string(Encode(v, false)); got != c[1] {
			t.Errorf("Encode(%s)\n got %s\nwant %s", c[0], got, c[1])
		}
		if got := string(Encode(v, true)); got != c[2] {
			t.Errorf("Encode sorted(%s)\n got %s\nwant %s", c[0], got, c[2])
		}
	}
}

func TestFloatRepr(t *testing.T) {
	for f, want := range map[float64]string{
		0: "0.0", 1: "1.0", -2.5: "-2.5", 0.1: "0.1", 1e16: "1e+16", 1e15: "1000000000000000.0",
		1e-4: "0.0001", 1e-5: "1e-05", 123.456: "123.456", 1.5e300: "1.5e+300",
		math.Inf(1): "Infinity", math.Copysign(0, -1): "-0.0", 1.7976931348623157e308: "1.7976931348623157e+308",
	} {
		if got := FloatRepr(f); got != want {
			t.Errorf("FloatRepr(%v) = %s, want %s", f, got, want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, in := range []string{``, `{`, `[1,]`, `{"a":1} x`, `nul`} {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("Parse(%q): no error", in)
		}
	}
}

func TestObjectGet(t *testing.T) {
	v, _ := Parse([]byte(`{"id":"C1","n":2,"id":"C2"}`))
	obj := v.(*Object)
	if id, _ := obj.Get("id"); id != "C2" {
		t.Errorf("id = %v", id)
	}
	if _, ok := obj.Get("missing"); ok {
		t.Error("missing key found")
	}
	if s, _ := Text(Number("10")); s != "10" {
		t.Errorf("Text(10) = %s", s)
	}
}
