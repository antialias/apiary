package main

import (
	"net/http"
	"testing"
)

// A retryable reply with no time (an account no (unknown) with no next poll,
// or a time already past) answers without a Retry-After, on the Anthropic
// and the OpenAI wire. The session's funding note used to read AvailableAt
// whenever the reply was retryable: a nil dereference inside RoundTrip.
func TestUnavailableResponseRetryableUntimed(t *testing.T) {
	yes := true
	why := "unknown"
	u := &unavailable{tenant: lan, Retryable: &yes, Options: []unavailOption{},
		Members: []unavailMember{{Route: "frontier/gpt/gpt-5.6", Accounts: []unavailAccount{
			{Account: "gpt", Check: "no", Why: &why, Basis: "unknown"}}}}}
	for _, openai := range []bool{false, true} {
		cc := &classRoute{tenant: lan, class: "frontier", session: "untimed-test", origHeader: http.Header{}, unavail: u}
		cc.wireOpenAI = openai
		resp := lan.unavailableResponse(cc)
		if resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-Switch-Available-At") != "unknown" {
			t.Fatalf("openai=%v: Retry-After %q, X-Switch-Available-At %q, want none and unknown",
				openai, resp.Header.Get("Retry-After"), resp.Header.Get("X-Switch-Available-At"))
		}
		if want := map[bool]int{false: 529, true: 429}[openai]; resp.StatusCode != want {
			t.Fatalf("openai=%v: status %d, want %d", openai, resp.StatusCode, want)
		}
	}
}
