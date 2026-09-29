// plugin-contract checks first-party Object entry sources, not arbitrary native code.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

type Input struct{ Files map[string]string }

func inspect(name, source string) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		return []string{err.Error()}
	}
	imports := map[string]string{}
	var errors []string
	for _, item := range file.Imports {
		value, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			errors = append(errors, "invalid import")
			continue
		}
		alias := path.Base(value)
		if item.Name != nil {
			alias = item.Name.Name
		}
		if alias == "." {
			errors = append(errors, name+": dot imports are not auditable")
			continue
		}
		if value == "net/rpc" || value == "net/rpc/jsonrpc" || value == "os/exec" || value == "plugin" || value == "syscall" || value == "golang.org/x/sys/unix" {
			errors = append(errors, name+": forbidden effect import "+value)
		}
		imports[alias] = value
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if !ok || ident.Obj != nil {
			return true
		}
		pkg := imports[ident.Name]
		method := selector.Sel.Name
		forbidden := false
		switch pkg {
		case "net":
			forbidden = strings.HasPrefix(method, "Dial") || strings.HasPrefix(method, "Listen") || strings.HasPrefix(method, "Lookup")
		case "net/http":
			forbidden = method == "DefaultClient" || method == "DefaultTransport" || method == "Get" || method == "Post" || method == "PostForm" || method == "Head" || method == "Client" || method == "Transport" || strings.Contains(method, "ListenAndServe") || strings.HasPrefix(method, "Serve") || method == "FileServer" || method == "Dir"
		case "google.golang.org/grpc":
			forbidden = method == "NewClient" || method == "Dial" || method == "DialContext" || method == "NewServer"
		case "os":
			forbidden = !(strings.HasPrefix(method, "Err") || strings.HasPrefix(method, "Mode") || method == "PathSeparator" || method == "PathListSeparator")
		case "io/ioutil":
			forbidden = true
		case "path/filepath":
			forbidden = method == "Glob" || method == "Walk" || method == "WalkDir" || method == "EvalSymlinks"
		case "database/sql":
			forbidden = method == "Open" || method == "OpenDB"
		case "github.com/lib/pq":
			forbidden = method == "NewConnector" || method == "DialOpen" || method == "Listener"
		}
		if forbidden {
			errors = append(errors, fmt.Sprintf("%s: direct effect %s.%s must use an injected Tool", fset.Position(selector.Pos()), pkg, method))
		}
		return true
	})
	return errors
}
func main() {
	var input Input
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 32<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(input.Files) == 0 {
		fmt.Fprintln(os.Stderr, "no Object entry source files selected")
		os.Exit(1)
	}
	failed := false
	for name, source := range input.Files {
		for _, message := range inspect(name, source) {
			fmt.Fprintln(os.Stderr, message)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
	fmt.Printf("PASS: %d Object entry sources; direct net/rpc/fs escape guard (not an OS sandbox)\n", len(input.Files))
}
