package river

import (
	"testing"
	"reflect"
)

func TestSplitToDelimiter(t *testing.T) {
	strTypes := []struct {
		Str       string
		Delimiter string
		Expect    []string
	} {
		{"AS_DF", "_", []string {"as", "df"}},
		{"qw-er", "-", []string {"qw", "er"}},
	}

	for _, strType := range strTypes {
		if !reflect.DeepEqual(splitToDelimiter(strType.Str, strType.Delimiter), strType.Expect) {
			t.Errorf("String: %s, Expected: is %s, but: was %s",
				strType.Str, strType.Expect, splitToDelimiter(strType.Str, strType.Delimiter))
		}
	}
}

func TestStringArrayToCamelCase(t *testing.T) {
	strTypes := []struct {
		Str    []string
		Expect string
	} {
		{[]string {"as", "df"}, "asDf"},
		{[]string {"qw", "er"}, "qwEr"},
	}

	for _, strType := range strTypes {
		if stringArrayToCamelCase(strType.Str) != strType.Expect {
			t.Errorf("String: %s, Expected: is %s, but: was %s",
				strType.Str, strType.Expect, stringArrayToCamelCase(strType.Str))
		}
	}
}
