package river

import (
	"testing"
)

func TestSliceContainsString(t *testing.T) {
	strTypes := []struct {
		Strs   []string
		Str    string
		Expect bool
	} {
		{[]string {"as", "df"}, "as", true},
		{[]string {"qw", "er"}, "zx", false},
	}

	for _, strType := range strTypes {
		if sliceContainsString(strType.Strs, strType.Str) != strType.Expect {
			t.Errorf("Slice: %s, String: %s, Expected: is %t, but: was %t",
				strType.Strs, strType.Str, strType.Expect, sliceContainsString(strType.Strs, strType.Str))
		}
	}
}