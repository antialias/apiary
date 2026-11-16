package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestReadImageToolResultsKeepPixelsOrderAndCallIdentity(t *testing.T) {
	m, _ := codexBodyMap(t, `{"model":"gpt-6-astra","messages":[{"role":"user","content":[
      {"type":"tool_result","tool_use_id":"read_png","content":[
        {"type":"text","text":"first page"},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":"cGl4ZWxz"}},
        {"type":"text","text":"second page"},
        {"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"bW9yZQ=="}}]},
      {"type":"tool_result","tool_use_id":"read_url","content":[
        {"type":"image","source":{"type":"url","url":"https://example.com/page.png"}}]}
    ]}]}`)
	items := m["input"].([]any)
	first := items[0].(map[string]any)
	want := []any{
		map[string]any{"type": "input_text", "text": "first page"},
		map[string]any{"type": "input_image", "image_url": "data:image/png;base64,cGl4ZWxz", "detail": "auto"},
		map[string]any{"type": "input_text", "text": "second page"},
		map[string]any{"type": "input_image", "image_url": "data:image/jpeg;base64,bW9yZQ==", "detail": "auto"},
	}
	if first["call_id"] != "read_png" || first["type"] != "function_call_output" || !reflect.DeepEqual(first["output"], want) {
		t.Fatalf("Read pixels or identity lost: %#v", first)
	}
	second := items[1].(map[string]any)
	if second["call_id"] != "read_url" || len(second["output"].([]any)) != 1 {
		t.Fatalf("image-only tool result lost: %#v", second)
	}
}

func TestImageToolFailureRetainsErrorAndRejectsMissingPixels(t *testing.T) {
	m, _ := codexBodyMap(t, `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"read","is_error":true,"content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}]}`)
	output := m["input"].([]any)[0].(map[string]any)["output"].([]any)
	if !strings.HasPrefix(output[0].(map[string]any)["text"].(string), "Error:") || output[1].(map[string]any)["type"] != "input_image" {
		t.Fatalf("error/image lost: %#v", output)
	}
	for _, content := range []string{`[{"type":"image"}]`, `[{"type":"image","source":{"type":"base64","media_type":"image/png","data":""}}]`} {
		_, _, _, err := translateAnthropicToCodex([]byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"read","content":` + content + `}]}]}`))
		if err == nil || !strings.Contains(err.Error(), "read") {
			t.Fatalf("missing image must fail explicitly, got %v", err)
		}
	}
}

func TestForwardedToolsKeepOptionalArgumentsOptional(t *testing.T) {
	const schemas = `[{"name":"Read","input_schema":{"type":"object","properties":{"file_path":{"type":"string"},"pages":{"type":"string"}},"required":["file_path"]}},
      {"name":"WebSearch","input_schema":{"type":"object","properties":{"query":{"type":"string"},"allowed_domains":{"type":"array","items":{"type":"string"}},"blocked_domains":{"type":"array","items":{"type":"string"}}},"required":["query"]}}]`
	var original []struct {
		InputSchema any `json:"input_schema"`
	}
	if err := json.Unmarshal([]byte(schemas), &original); err != nil {
		t.Fatal(err)
	}
	m, _ := codexBodyMap(t, `{"tools":`+schemas+`}`)
	for i, raw := range m["tools"].([]any) {
		tool := raw.(map[string]any)
		if tool["strict"] != false || !reflect.DeepEqual(tool["parameters"], original[i].InputSchema) {
			t.Fatalf("optional arguments changed: %#v", tool)
		}
	}
}
