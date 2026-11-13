// validate.go · valida el contrato v1 del websocket: cada ejemplo contra el sobre y contra el schema de su tipo
// conecta con: contract/schemas (sobre, mensaje del cliente y events/<tipo>) y contract/examples; lo corren E0 y la CI de importadoraRealtime
// uso: desde importadoraRealtime/ `go run ./contract/scripts/validate.go`; desde la raíz del repo `go -C importadoraRealtime run ./contract/scripts/validate.go`
// ojo: ubica contract/ por la ruta de este archivo, así que no depende de la carpeta desde donde se corre
// ojo: además exige que haya exactamente 48 schemas y 48 ejemplos con los mismos nombres y que el enum de `type` del sobre sea esa misma lista
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const expectedTypes = 48

func main() {
	root := contractDir()
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if err := run(root); err != nil {
		fmt.Fprintln(os.Stderr, "contrato inválido:", err)
		os.Exit(1)
	}
}

// contractDir es la carpeta contract/ (dos niveles arriba de este archivo: contract/scripts/validate.go)
func contractDir() string {
	if _, self, _, ok := runtime.Caller(0); ok {
		return filepath.Dir(filepath.Dir(self))
	}
	return "contract"
}

func run(root string) error {
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat() // date-time y demás formatos se validan de verdad

	envelope, envelopeDoc, err := compile(compiler, filepath.Join(root, "schemas", "envelope.schema.json"))
	if err != nil {
		return err
	}
	client, _, err := compile(compiler, filepath.Join(root, "schemas", "client-message.schema.json"))
	if err != nil {
		return err
	}

	schemaFiles, _ := filepath.Glob(filepath.Join(root, "schemas", "events", "*.schema.json"))
	exampleFiles, _ := filepath.Glob(filepath.Join(root, "examples", "*.json"))
	schemaTypes := namesOf(schemaFiles, ".schema.json")
	exampleTypes := namesOf(exampleFiles, ".json")
	if len(schemaTypes) != expectedTypes || len(exampleTypes) != expectedTypes {
		return fmt.Errorf("se esperaban %d schemas y %d ejemplos; hay %d y %d", expectedTypes, expectedTypes, len(schemaTypes), len(exampleTypes))
	}
	if strings.Join(schemaTypes, ",") != strings.Join(exampleTypes, ",") {
		return fmt.Errorf("los nombres de schemas y ejemplos no coinciden")
	}
	if err := sameAsEnvelopeEnum(envelopeDoc, schemaTypes); err != nil {
		return err
	}

	failures := 0
	for _, eventType := range schemaTypes {
		body, _, err := compile(compiler, filepath.Join(root, "schemas", "events", eventType+".schema.json"))
		if err != nil {
			return err
		}
		example, err := load(filepath.Join(root, "examples", eventType+".json"))
		if err != nil {
			return err
		}
		message, ok := example.(map[string]any)
		switch {
		case !ok:
			err = fmt.Errorf("no es un objeto")
		case message["type"] != eventType:
			err = fmt.Errorf("su type es %v y el archivo se llama %s", message["type"], eventType)
		default:
			if err = envelope.Validate(example); err == nil {
				err = body.Validate(message["data"])
			}
		}
		if err != nil {
			failures++
			fmt.Printf("✗ %s: %v\n", eventType, err)
		}
	}

	eventFailures := failures
	clientFiles, _ := filepath.Glob(filepath.Join(root, "examples", "client", "*.json"))
	for _, file := range clientFiles {
		message, err := load(file)
		if err == nil {
			err = client.Validate(message)
		}
		if err != nil {
			failures++
			fmt.Printf("✗ cliente %s: %v\n", filepath.Base(file), err)
		}
	}

	valid := expectedTypes - eventFailures
	if failures > 0 {
		return fmt.Errorf("%d/%d eventos válidos y %d/%d mensajes del cliente", valid, expectedTypes, len(clientFiles)-(failures-eventFailures), len(clientFiles))
	}
	fmt.Printf("%d/%d válidos (más %d mensajes del cliente)\n", valid, expectedTypes, len(clientFiles))
	return nil
}

// compile registra el schema con su $id y lo compila
func compile(compiler *jsonschema.Compiler, file string) (*jsonschema.Schema, map[string]any, error) {
	doc, err := load(file)
	if err != nil {
		return nil, nil, err
	}
	asMap, _ := doc.(map[string]any)
	id, _ := asMap["$id"].(string)
	if id == "" {
		return nil, nil, fmt.Errorf("%s no tiene $id", file)
	}
	if err := compiler.AddResource(id, doc); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", file, err)
	}
	schema, err := compiler.Compile(id)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", file, err)
	}
	return schema, asMap, nil
}

func load(file string) (any, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	return doc, nil
}

func namesOf(files []string, suffix string) []string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, strings.TrimSuffix(filepath.Base(f), suffix))
	}
	sort.Strings(names)
	return names
}

// sameAsEnvelopeEnum revisa que el enum de `type` del sobre sea exactamente la lista de schemas
func sameAsEnvelopeEnum(envelope map[string]any, types []string) error {
	props, _ := envelope["properties"].(map[string]any)
	typeProp, _ := props["type"].(map[string]any)
	raw, _ := typeProp["enum"].([]any)
	listed := make([]string, 0, len(raw))
	for _, item := range raw {
		listed = append(listed, fmt.Sprint(item))
	}
	sort.Strings(listed)
	if strings.Join(listed, ",") != strings.Join(types, ",") {
		return fmt.Errorf("el enum de type del sobre (%d) no coincide con los schemas de events/ (%d)", len(listed), len(types))
	}
	return nil
}
