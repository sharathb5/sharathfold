// Command checkserialize fails if worker-path files import or call encoding/json.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: checkserialize <file>...")
		os.Exit(2)
	}
	fset := token.NewFileSet()
	failed := false
	for _, path := range os.Args[1:] {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invariant FAIL: parse %s: %v\n", path, err)
			failed = true
			continue
		}
		for _, imp := range f.Imports {
			if imp.Path.Value == `"encoding/json"` {
				fmt.Fprintf(os.Stderr, "invariant FAIL: %s imports encoding/json (no serialization in worker path)\n", path)
				failed = true
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "json" {
				return true
			}
			switch sel.Sel.Name {
			case "Marshal", "MarshalIndent", "NewEncoder", "NewDecoder", "Unmarshal":
				pos := fset.Position(sel.Pos())
				fmt.Fprintf(os.Stderr, "invariant FAIL: %s:%d: json.%s in worker path\n", pos.Filename, pos.Line, sel.Sel.Name)
				failed = true
			}
			return true
		})
	}
	if failed {
		os.Exit(1)
	}
}
