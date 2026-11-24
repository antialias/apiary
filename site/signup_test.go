package main

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// Every question on the waggle list is a field the server keeps: a question it
// dropped would be answered and silently lost. And the plan page's links into
// the form tick choices that exist.
func TestWaggleListKeepsEveryAnswer(t *testing.T) {
	pages, err := fs.Sub(static, "static")
	if err != nil {
		t.Fatal(err)
	}
	about, _ := fs.ReadFile(pages, "about/index.html")
	form := regexp.MustCompile(`(?s)<form id="f".*?</form>`).Find(about)
	if form == nil {
		t.Fatal("about: no form")
	}
	kept := map[string]bool{}
	st := reflect.TypeOf(signup{})
	for i := 0; i < st.NumField(); i++ {
		kept[strings.Split(st.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	var asked []string
	for _, m := range regexp.MustCompile(`(?:\bname|data-name)="([a-z_]+)"`).FindAllSubmatch(form, -1) {
		if name := string(m[1]); name != "website" { // the honeypot, dropped on purpose
			asked = append(asked, name)
			if !kept[name] {
				t.Errorf("the form asks %q but the server doesn't keep it", name)
			}
		}
	}
	if len(asked) < 7 {
		t.Errorf("found only %v in the form", asked)
	}

	plan, _ := fs.ReadFile(pages, "plan/index.html")
	for _, m := range regexp.MustCompile(`href="(/[^"]*)#join"`).FindAllSubmatch(plan, -1) {
		link := string(m[1])
		if !strings.HasPrefix(link, "/about/") {
			t.Errorf("plan links to %s#join; the form is on /about/", link)
		}
		if want := regexp.MustCompile(`want=([a-z-]+)`).FindSubmatch(m[1]); want != nil &&
			!strings.Contains(string(form), `type="checkbox" value="`+string(want[1])+`"`) {
			t.Errorf("plan links to want=%s, which the form has no choice for", want[1])
		}
	}

	file := filepath.Join(t.TempDir(), "signups.jsonl")
	h := site(pages, file)
	body := `{"email":"bee@example.com","handle":"bee","uses":["claude-plan","local"],"plan_tier":"two plans",
		"hardware":"4090","spend":"$100–300","pool":["share","draw"],"pain":"runs dry mid-run"}`
	req := httptest.NewRequest(http.MethodPost, "https://apiary.wtf/signup", strings.NewReader(body))
	req.RemoteAddr = "192.0.2.7:4000"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("signup: %d %s", rec.Code, rec.Body)
	}
	line, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var got signup
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Uses, []string{"claude-plan", "local"}) || !reflect.DeepEqual(got.Pool, []string{"share", "draw"}) ||
		got.PlanTier != "two plans" || got.Hardware != "4090" || got.Spend != "$100–300" || got.Pain != "runs dry mid-run" {
		t.Errorf("stored %s", line)
	}
}
