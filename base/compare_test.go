package base

import (
	"bytes"
	"testing"

	"github.com/buger/jsonparser"
)

func TestJsonParser(t *testing.T) {
	tests := []struct {
		json string
		path []string

		err          string
		outValue     string
		outValueType jsonparser.ValueType
		outOffset    int
	}{
		{
			json:         `123`,
			path:         []string{},
			outValue:     `123`,
			outValueType: jsonparser.Number,
			outOffset:    3,
		},
		{
			json:         `   123`,
			path:         []string{},
			outValue:     `123`,
			outValueType: jsonparser.Number,
			outOffset:    6,
		},
		{
			json:         `   `,
			path:         []string{},
			err:          "expected some bad json error",
			outValue:     ``,
			outValueType: jsonparser.NotExist,
			outOffset:    -1,
		},
		{
			json:         `   " hello world "  `,
			path:         []string{},
			outValue:     ` hello world `,
			outValueType: jsonparser.String,
			outOffset:    18,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{},
			outValue:     `[ "hello", [] ]`,
			outValueType: jsonparser.Array,
			outOffset:    18,
		},
		{
			json:         `   [ "hello", []   `,
			path:         []string{},
			err:          "should detect a missing closing bracket",
			outValue:     ``,
			outValueType: jsonparser.Array,
			outOffset:    3,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[0]"},
			outValue:     `hello`,
			outValueType: jsonparser.String,
			outOffset:    12,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[1]"},
			outValue:     `[]`,
			outValueType: jsonparser.Array,
			outOffset:    16,
		},
		{
			json:         `   [ "hello", [] ]  `,
			path:         []string{"[2]"},
			err:          "should have error as array is too short",
			outValue:     ``,
			outValueType: jsonparser.NotExist,
			outOffset:    -1,
		},
		{
			json:         `   "hello  `,
			path:         []string{},
			err:          "should detect a missing closing string",
			outValue:     ``,
			outValueType: jsonparser.String,
			outOffset:    3,
		},
		{
			json:         `   " hello\"world "  `,
			path:         []string{},
			outValue:     " hello\\\"world ",
			outValueType: jsonparser.String,
			outOffset:    19,
		},
	}

	for testi, test := range tests {
		value, valueType, offset, err := jsonparser.Get([]byte(test.json), test.path...)
		if (err != nil) != (test.err != "") {
			t.Fatalf("testi: %d, test: %+v, err: %v",
				testi, test, err)
		}

		if string(value) != test.outValue {
			t.Fatalf("testi: %d, test: %+v, wrong value: %s",
				testi, test, value)
		}

		if valueType != test.outValueType {
			t.Fatalf("testi: %d, test: %+v, wrong valueType: %d",
				testi, test, valueType)
		}

		if offset != test.outOffset {
			t.Fatalf("testi: %d, test: %+v, wrong offset: %d",
				testi, test, offset)
		}
	}
}

func TestJsonParserUnescape(t *testing.T) {
	v, err := jsonparser.Unescape([]byte(` hello\"world `), nil)
	if err != nil {
		t.Errorf("not expecting err")
	}
	if string(v) != ` hello"world ` {
		t.Errorf("got: %s, %#v", v, v)
	}
}

func TestValComparer(t *testing.T) {
	testValComparer(t, nil)
}

func TestValComparerReuse(t *testing.T) {
	c := NewValComparer()
	c.PrepareEncoder()

	testValComparer(t, c)
}

func testValComparer(t *testing.T, vIn *ValComparer) {
	tests := []struct {
		a string
		b string
		c int
	}{
		{
			a: ``,
			b: ``,
			c: 0,
		},
		{
			a: ``,
			b: `123`,
			c: -1,
		},
		{
			a: `123`,
			b: ``,
			c: 1,
		},
		{
			a: `99`,
			b: `123`,
			c: -1,
		},
		{
			a: `123`,
			b: `99`,
			c: 1,
		},
		{
			a: `123`,
			b: `123`,
			c: 0,
		},
		{
			a: `[123]`,
			b: `[123]`,
			c: 0,
		},
		{
			a: `[1,2,3]`,
			b: `[1,2,3]`,
			c: 0,
		},
		{
			a: `[1,2]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[1,2,1]`,
			b: `[1,2,3]`,
			c: -1,
		},
		{
			a: `[1,2,3,0]`,
			b: `[1,2,3]`,
			c: 1,
		},
		{
			a: `[1,2,"b"]`,
			b: `[1,2,"a"]`,
			c: 1,
		},
		{
			a: `[1,2,3,0]`,
			b: `[]`,
			c: 1,
		},
		{
			a: ` [ 1 ,   2  , 3 , 0 ] `,
			b: `  [  ] `,
			c: 1,
		},
		{
			a: `{}`,
			b: `{}`,
			c: 0,
		},
		{
			a: `{"a":1}`,
			b: `{"a":1}`,
			c: 0,
		},
		{
			a: `{"a":"y"}`,
			b: `{"a":"x"}`,
			c: 1,
		},
		{
			a: `{"a\"X":"y"}`,
			b: `{"a\"X":"x"}`,
			c: 1,
		},
		{
			a: `{"a\"X":"y\"Y\"Y"}`,
			b: `{"a\"X":"x\"X\"X"}`,
			c: 1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"a":1}`,
			c: 0,
		},
		{
			a: `{"a":1,"b":2,"c":3}`,
			b: `{"b":2,"a":1}`,
			c: 1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"a":1,"c":3}`,
			c: -1,
		},
		{
			a: `{"c":1,"b":2}`,
			b: `{"b":2,"a":1}`,
			c: -1,
		},
		{
			a: `{"a":1,"b":2}`,
			b: `{"b":2,"c":1}`,
			c: 1,
		},
		{
			a: `{"a":1,"b\"B":2}`,
			b: `{"b\"B":2,"c":1}`,
			c: 1,
		},
	}

	for testi, test := range tests {
		v := vIn
		if v == nil {
			v = NewValComparer()
		}

		c := v.Compare([]byte(test.a), []byte(test.b))
		if c != test.c {
			t.Fatalf("testi: %d, test: %+v, c: %d",
				testi, test, c)
		}
	}
}

func BenchmarkValCompare(b *testing.B) {
	v := NewValComparer()

	x := []byte(`[1,"2",[]]`)
	y := []byte(`[1,"2",[]]`)

	v.Compare(x, y)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v.Compare(x, y)
	}
}

func BenchmarkEncodeAsString(b *testing.B) {
	v := NewValComparer()
	o := make([]byte, 0, 1000)

	s := []byte("hello\"world")
	j := []byte(`"hello\"world"`)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		out, err := v.EncodeAsString(s, o)
		if err != nil {
			b.Fatalf("err: %v", err)
		}

		if !bytes.Equal(j, out) {
			b.Fatalf("not equal")
		}
	}
}

func BenchmarkParse(b *testing.B) {
	x := []byte(`"hello\"world"`)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Parse(x)
	}
}

// TestValComparerObjectDoesNotCorrupt guards against a regression where
// canonicalizing or comparing an OBJECT value left the shared ValComparer's
// depth-pool in a corrupted state (KeyValsRelease was called before the sort +
// loop that still used the pooled slice), making a *subsequent* Compare on the
// same comparer return the wrong result. That surfaced as rows being dropped
// after an ORDER BY / DISTINCT / GROUP BY on an object-valued key.
func TestValComparerObjectDoesNotCorrupt(t *testing.T) {
	obj := Val(`{"name":"1200","zzz":{"a":1,"b":2}}`)
	str := Val(`"select_func"`)

	// CanonicalJSON(object) must not corrupt a later Compare.
	vc := &ValComparer{}
	if vc.Compare(str, str) != 0 {
		t.Fatal("baseline: equal strings should compare 0")
	}
	_, err := vc.CanonicalJSON(obj, nil)
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if got := vc.Compare(str, str); got != 0 {
		t.Errorf("Compare after object CanonicalJSON = %d, want 0 (comparer corrupted)", got)
	}
	if got := vc.Compare(Val(`"a"`), Val(`"b"`)); got >= 0 {
		t.Errorf("Compare(a,b) after object CanonicalJSON = %d, want <0", got)
	}

	// Compare(object,...) must not corrupt a later Compare either.
	vc2 := &ValComparer{}
	_ = vc2.Compare(obj, Val(`{"name":"1234","zzz":{"a":1,"b":9}}`))
	if got := vc2.Compare(str, str); got != 0 {
		t.Errorf("Compare after object Compare = %d, want 0 (comparer corrupted)", got)
	}

	// Object comparison itself stays correct.
	if got := vc.Compare(Val(`{"name":"1234"}`), Val(`{"name":"1235"}`)); got >= 0 {
		t.Errorf("Compare distinct objects = %d, want <0", got)
	}
}

// TestStringCompareDoesNotPoisonPool guards against a regression where a String
// CompareWithType stored its (possibly input-aliasing) unescaped operands back
// into the reusable KeyVals pool. When jsonparser.Unescape finds no escapes it
// returns the input slice unchanged, so the pool ended up holding pointers to
// caller memory (e.g. a static constant); a later Object canonical/compare then
// ReuseNextKey'd that slot and overwrote the constant. Manifested as rows being
// dropped after an ORDER BY / DISTINCT on an object key (the WHERE constant got
// clobbered by an object field name).
func TestStringCompareDoesNotPoisonPool(t *testing.T) {
	vc := &ValComparer{}
	konst := Val(`"select_func"`) // a stable "constant" the caller must keep intact

	// A String compare against the constant (as a WHERE eq would do).
	if vc.Compare(konst, Val(`"array_func"`)) == 0 {
		t.Fatal("baseline: distinct strings should not compare equal")
	}
	// Now canonicalize an object whose key would be copied via ReuseNextKey.
	if _, err := vc.CanonicalJSON(Val(`{"name":"x"}`), nil); err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	// The constant must be untouched.
	if string(konst) != `"select_func"` {
		t.Errorf("constant was corrupted to %q (pool poisoning regression)", string(konst))
	}
	if vc.Compare(konst, Val(`"select_func"`)) != 0 {
		t.Errorf("constant no longer compares equal to itself after object canonicalize")
	}
}

// TestArrayCompareIsAntisymmetric guards a bug where comparing arrays whose
// FIRST differing element decided the result (with more elements following)
// let the "shorter array is less" rule override the real comparison: the
// element loop stops advancing its index once cmp != 0, so i < bLen stayed
// true and Compare returned -1 regardless of the actual order. Manifested as
// multi-key ORDER BY on an array-valued sort key (e.g. OBJECT_PAIRS(...))
// mis-sorting rows -- Compare(a,b) and Compare(b,a) both returned -1.
func TestArrayCompareIsAntisymmetric(t *testing.T) {
	vc := &ValComparer{}

	cases := [][2]string{
		{`["2011","zzz"]`, `["2015","zzz"]`},         // strings, decide on elem 0
		{`[2011,9]`, `[2015,9]`},                     // numbers, decide on elem 0
		{`["x","a"]`, `["x","b"]`},                   // decide on elem 1
		{`[{"n":"a","v":"1"},{"n":"b","v":"x"}]`, `[{"n":"a","v":"2"},{"n":"b","v":"x"}]`}, // objects
		{`["x"]`, `["x","y"]`},                       // equal prefix, a shorter -> a < b
	}
	for _, c := range cases {
		a, b := Val(c[0]), Val(c[1])
		if ab, ba := vc.Compare(a, b), vc.Compare(b, a); ab != -1 || ba != 1 {
			t.Errorf("Compare(%s,%s)=%d, Compare reversed=%d; want -1 and +1",
				c[0], c[1], ab, ba)
		}
	}

	// Equal arrays compare equal both ways.
	eq := Val(`[1,2,3]`)
	if vc.Compare(eq, Val(`[1,2,3]`)) != 0 {
		t.Errorf("equal arrays should compare equal")
	}
}
