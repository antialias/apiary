//go:build lesson_live

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Explicit opt-in probe. Run in an isolated container with the proxy's existing
// read-only key mount; it neither refreshes credentials nor changes routing.
func TestLiveLessonToolContract(t *testing.T) {
	brainURL = "http://192.0.2.10:9115"
	key, err := os.ReadFile("/app/switch_key")
	if err != nil {
		t.Fatal(err)
	}
	switchKey = strings.TrimSpace(string(key))
	tok, acct, _, ok := getCodexToken("")
	if !ok {
		t.Fatal("brain did not supply a bearer")
	}
	palette := []color.RGBA{{255, 0, 0, 255}, {0, 180, 0, 255}, {0, 0, 255, 255}, {255, 255, 0, 255}}
	names := []string{"red", "green", "blue", "yellow"}
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	for i := 3; i > 0; i-- {
		j := int(random[i]) % (i + 1)
		palette[i], palette[j] = palette[j], palette[i]
		names[i], names[j] = names[j], names[i]
	}
	pic := image.NewRGBA(image.Rect(0, 0, 640, 160))
	for i, c := range palette {
		draw.Draw(pic, image.Rect(i*160, 0, (i+1)*160, 160), &image.Uniform{c}, image.Point{}, draw.Src)
	}
	var pngBytes bytes.Buffer
	if err := png.Encode(&pngBytes, pic); err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(pngBytes.Bytes())
	for _, model := range []string{"gpt-6-astra", "gpt-6.1-sol"} {
		t.Run(model+"/image", func(t *testing.T) {
			request := map[string]any{"model": model, "system": "Follow the user's request. Be concise.", "messages": []any{
				map[string]any{"role": "user", "content": "Read the image and name its four solid colors from left to right. Reply with just the four lowercase English color names, comma-separated."},
				map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "read_fixture", "name": "Read", "input": map[string]any{"file_path": "/tmp/puzzle.png"}}}},
				map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "read_fixture", "content": []any{map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": encoded}}}}}},
			}}
			reply := liveLessonRequest(t, tok, acct, request)
			var text strings.Builder
			for _, item := range reply {
				if item.Type == "message" {
					for _, part := range item.Content {
						text.WriteString(part.Text)
					}
				}
			}
			got := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(text.String()), " ", ""))
			if strings.TrimRight(got, ".\n") != strings.Join(names, ",") {
				t.Fatalf("pixels not read: %q; expected %v", text.String(), names)
			}
			t.Logf("%s read tool-result pixels correctly", model)
		})
		for _, tool := range []string{"Read", "WebSearch"} {
			t.Run(model+"/"+tool, func(t *testing.T) {
				schema := map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}, "pages": map[string]any{"type": "string"}}, "required": []string{"file_path"}}
				prompt := "Read /tmp/puzzle.png."
				if tool == "WebSearch" {
					schema = map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "allowed_domains": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "blocked_domains": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}}, "required": []string{"query"}}
					prompt = "Search the web for violin jaw rest teaching references."
				}
				request := map[string]any{"model": model, "system": "Use the requested tool.", "messages": []any{map[string]any{"role": "user", "content": prompt}}, "tools": []any{map[string]any{"name": tool, "input_schema": schema}}, "tool_choice": map[string]any{"type": "tool", "name": tool}}
				reply := liveLessonRequest(t, tok, acct, request)
				found := false
				for _, item := range reply {
					if item.Type == "function_call" {
						found = true
						var args map[string]any
						if err := json.Unmarshal([]byte(item.Arguments), &args); err != nil {
							t.Fatal(err)
						}
						for _, optional := range []string{"pages", "allowed_domains", "blocked_domains"} {
							if _, has := args[optional]; has {
								t.Fatalf("unrequested optional %s in %s", optional, item.Arguments)
							}
						}
						t.Logf("%s %s: optional arguments omitted", model, tool)
					}
				}
				if !found {
					t.Fatal("no tool call returned")
				}
			})
		}
	}
}

func liveLessonRequest(t *testing.T, tok, acct string, request map[string]any) []codexItem {
	t.Helper()
	raw, _ := json.Marshal(request)
	body, _, _, err := translateAnthropicToCodex(raw)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", "https://chatgpt.com"+codexResponsesPath, bytes.NewReader(body))
	req.Header = codexHeaders(tok, acct, "violin-compatibility-probe")
	response, err := (&http.Client{Timeout: 120 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		t.Fatalf("HTTP %d: %s", response.StatusCode, body)
	}
	var items []codexItem
	complete := false
	err = parseSSE(response.Body, func(data []byte) bool {
		var event codexSSEEvent
		if json.Unmarshal(data, &event) != nil {
			return true
		}
		if event.Type == "error" || event.Type == "response.failed" {
			t.Errorf("upstream refusal: %.1800s", data)
			return false
		}
		if event.Type == "response.output_item.done" && event.Item != nil {
			items = append(items, *event.Item)
		}
		if event.Type == "response.completed" {
			complete = true
			return false
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("response did not complete")
	}
	return items
}
