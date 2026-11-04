package main

import "testing"

func TestRejectsAliasedAndIndirectDirectEffects(t *testing.T) {
	for _, source := range []string{
		"package main; import n \"net\"; var dial=n.Dial",
		"package main; import \"os\";func f(){os.ReadFile(\"secret\")}",
		"package main; import _ \"net/rpc\"",
		"package main; import . \"net/http\"",
		"package main; import \"net/http\";var c=http.DefaultClient",
		"package main; import \"database/sql\";func f(){sql.Open(\"postgres\",\"dsn\")}",
	} {
		if len(inspect("object.go", source)) == 0 {
			t.Errorf("accepted bypass: %s", source)
		}
	}
}
func TestAcceptsDataTypesAndCallbackCalls(t *testing.T) {
	source := "package main; import \"net/http\";type F func([]byte)([]byte,error);func f(tool F,h http.Header){tool([]byte(h.Get(\"X\")))}"
	if errors := inspect("object.go", source); len(errors) != 0 {
		t.Fatal(errors)
	}
	if errors := inspect("object.go", "not Go source"); len(errors) == 0 {
		t.Fatal("accepted unparsable source")
	}
}
