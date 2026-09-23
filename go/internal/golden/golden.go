// Package golden reads testdata/golden/manifest.json, the cross-language
// contract the decoders in this repository are held to.
//
// The decoders themselves are deliberately not shared: three parsers reading
// the same bytes from the same field tables, independently, is what makes a
// misreading of the spec on one side fail against the others. A JSON reader
// carries none of that value. There is one manifest and one schema for it, and
// transcribing the same struct tags into three modules only creates three
// places for the schema to drift.
package golden

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// Vector is one entry of the manifest's `vectors` array, reduced to the keys a
// suite holds itself to. The rest — `message`, `feed`, `note`, `lowered_from`,
// `spec_revision` — is prose about a vector rather than a value to assert.
type Vector struct {
	File          string `json:"file"`
	TypeID        string `json:"type_id"`
	Size          int    `json:"size"`
	SchemaVersion uint8  `json:"schema_version"`
	FlagsOnWire   uint16 `json:"flags_on_wire"`
	// Raw, so that a nanosecond timestamp is read as the integer it is. Decoded
	// into interface{} it would become a float64 and 1700000000000000003 would
	// compare equal to 1700000000000000002.
	Fields map[string]json.RawMessage `json:"fields"`
}

type manifest struct {
	Vectors []Vector `json:"vectors"`
}

// Field is one row of a vector's `fields` block: the manifest's name for it,
// the value the suite decoded, and the value the manifest states.
//
// The members are exported so that a suite can alias this type and keep its
// rows as positional literals.
type Field struct {
	Name string
	Got  int64
	Want int64
}

// Text is a Field for the fixed-width ASCII fields, trimmed of their null
// padding by the time they get here.
type Text struct {
	Name string
	Got  string
	Want string
}

// Read parses dir/manifest.json and returns its vectors by file name.
//
// A repeated `file` is refused rather than collapsed: the second entry would
// silently replace the first, so a manifest stating two different sets of
// values for one vector would leave every suite green.
func Read(t *testing.T, dir string) map[string]Vector {
	t.Helper()
	path := filepath.Join(dir, "manifest.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("parse manifest.json: %v", err)
	}
	if len(m.Vectors) == 0 {
		t.Fatalf("manifest.json lists no vectors")
	}
	byFile := make(map[string]Vector, len(m.Vectors))
	for _, v := range m.Vectors {
		if _, dup := byFile[v.File]; dup {
			t.Fatalf("manifest.json lists %s twice", v.File)
		}
		byFile[v.File] = v
	}
	return byFile
}

// RequireEveryVectorIsStated holds the corpus to the directory rather than only
// to the manifest: every `.bin` in dir must have a manifest entry.
//
// Without it the binding runs one way only. Every other check here iterates the
// manifest, so a vector committed to testdata/golden and named in no manifest
// row is read by no suite in either language and fails nothing — which is the
// one thing README.md says the corpus does not allow.
func RequireEveryVectorIsStated(t *testing.T, dir string, stated map[string]Vector) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".bin" {
			continue
		}
		if _, ok := stated[e.Name()]; !ok {
			t.Errorf("%s is in %s with no manifest.json entry, so no suite in either language reads it: add a row for it, or delete the file", e.Name(), dir)
		}
	}
}

// CheckFields compares one vector's `fields` block with the rows a suite
// asserts, both ways round.
func CheckFields(t *testing.T, m Vector, fields []Field, text []Text) {
	t.Helper()
	asserted := make(map[string]bool, len(fields)+len(text))
	for _, f := range fields {
		asserted[f.Name] = true
		raw, ok := m.Fields[f.Name]
		if !ok {
			t.Errorf("fields has no %s, which this suite asserts as %d", f.Name, f.Want)
			continue
		}
		got, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			t.Errorf("fields.%s = %s, which is not an integer: %v", f.Name, raw, err)
			continue
		}
		if got != f.Want {
			t.Errorf("fields.%s = %d, but this suite asserts %d", f.Name, got, f.Want)
		}
	}
	for _, f := range text {
		asserted[f.Name] = true
		raw, ok := m.Fields[f.Name]
		if !ok {
			t.Errorf("fields has no %s, which this suite asserts as %q", f.Name, f.Want)
			continue
		}
		var got string
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("fields.%s = %s, which is not a string: %v", f.Name, raw, err)
			continue
		}
		if got != f.Want {
			t.Errorf("fields.%s = %q, but this suite asserts %q", f.Name, got, f.Want)
		}
	}
	// The other direction. A field added to the manifest and asserted nowhere
	// is a value nothing holds the decoder to, which is exactly what this
	// test refuses to let the manifest carry.
	var unasserted []string
	for name := range m.Fields {
		if !asserted[name] {
			unasserted = append(unasserted, name)
		}
	}
	sort.Strings(unasserted)
	for _, name := range unasserted {
		t.Errorf("fields.%s = %s, which this suite asserts nowhere", name, m.Fields[name])
	}
}
