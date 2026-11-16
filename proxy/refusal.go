package main

// ---- one refusal taxonomy (quota-model 50 §2.6, C7) ----
//
// Every provider adapter maps a refusal (status, body, headers) to one class,
// and the class alone decides whether the walk moves on and what the record
// store keeps (records.go). Status alone never decides: Runway says "no money"
// with a 400, Kimi says "no membership" with a 403, codex says "no money"
// with a 429 that is not a rate limit. Before C7 the walk gate was "429
// always, 403 only for providers whose message has quota words", and a list
// of phrases decided whether a refusal was a budget fact at all; both are
// gone. The words an upstream uses are still how some of them say what they
// mean (Kimi's wordings, 12 §4), so an adapter reads them, for its provider
// only.
//
//	class         walks   record
//	spent         yes     (account, limit): a window, a cycle pool, a rolling count
//	unpaid        yes     (account, pot)
//	not_entitled  yes     (account, entitlement), or (account, model)
//	throttled     yes     short: Retry-After, else SWITCH_429_COOLDOWN; deranks
//	wrong_member  yes     none: this member can't take this request
//	degraded      health  none here: the health ladder (classify.go rows)
//	invalid       no      none: the request itself is bad; the client gets it
//
// A refusal that is none of these (an auth failure on a wire that re-auths,
// a 404 off the codex wire) has no class here and keeps classify's own row.

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// The classes (§2.6).
const (
	refSpent       = "spent"
	refUnpaid      = "unpaid"
	refNotEntitled = "not_entitled"
	refThrottled   = "throttled"
	refDegraded    = "degraded"
	refWrongMember = "wrong_member"
	refInvalid     = "invalid"
)

// limitEntitlement keys a not_entitled refusal about the account's plan (its
// membership), not one model.
const limitEntitlement = "entitlement"

// What a refused limit is, for when its record ends (records.go).
const (
	limWindow      = "window"      // an allowance window: attributed when the account's reading carries it
	limPool        = "pool"        // a cycle pool or a count no reading maps yet: unattributed
	limPot         = "pot"         // a money pot
	limCount       = "count"       // a pot's rolling count: attributed when the account's reading carries it
	limEntitlement = "entitlement" // the plan itself
	limModel       = "model"       // one model on the account: never attributed
)

// refLimit is one limit a refusal names.
type refLimit struct {
	name string // the record's limit key: "five_hour", "scoped:Fable", "credits", "entitlement", "model:<m>"
	kind string // lim*
	// readable says, for a pot or the entitlement, that a reading of this
	// provider shows it (the codex credits gate, Runway's balance, Kimi's
	// membership), so the record ends by reading (§2.6 "Attributed"). A
	// window is attributed when the account's reading carries it, decided
	// when the record is written.
	readable bool
	resets   time.Time // the reset the refusal names for this limit; zero = none
}

// refusal is one refusal, classified.
type refusal struct {
	class  string
	limits []refLimit // spent, unpaid, not_entitled: what refused; nil otherwise
	detail string     // the upstream's own words, one bounded line
	// code is the upstream's own error code (codex error.code), "" when it
	// sent none: an invalid refusal reaches the client with it.
	code string
	body []byte // the buffered body (nil when none was read)
}

// walks: the classes the walk moves on from (§2.6). degraded is the health
// ladder's, and invalid goes back to the client.
func (r refusal) walks() bool {
	switch r.class {
	case refSpent, refUnpaid, refNotEntitled, refThrottled, refWrongMember:
		return true
	}
	return false
}

// records: the classes that write a record (wrong_member and invalid are no
// usage fact about anyone).
func (r refusal) records() bool { return r.walks() && r.class != refWrongMember }

// rungRefusal classifies member m's response by its provider's adapter. It
// buffers the body only for the statuses an adapter reads, so a 200 stream is
// never touched; nil and 2xx-3xx are no refusal.
func rungRefusal(m classMemberPref, resp *http.Response) refusal {
	if resp == nil || resp.StatusCode < 400 {
		return refusal{}
	}
	now := nowFn()
	switch memberWireName(m) {
	case "anthropic":
		return anthropicRefusal(resp, m.Model, now)
	case "codex":
		return codexRefusal(resp, m.Model, now)
	case "bearer":
		return kimiRefusal(resp, m.Model, now)
	}
	if p := providerByName(m.Provider); m.Kind == "provider" && p != nil {
		if rw, ok := p.wire.(refusingWire); ok {
			return rw.refusal(resp, m.Model)
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		b := bufferResp(resp)
		return refusal{class: refThrottled, detail: "429", body: b}
	}
	return refusal{}
}

// refDetail is the upstream's message as one bounded line, else the status.
func refDetail(body []byte, fallback string) string {
	_, msg := errBodyTypeMsg(body)
	return refDetailText(msg, fallback)
}

// refDetailText is refDetail's rendering of a message already read.
func refDetailText(msg, fallback string) string {
	if msg == "" {
		return fallback
	}
	msg = strings.Join(strings.Fields(msg), " ")
	if len(msg) > 140 {
		msg = msg[:139] + "…"
	}
	return msg
}

// anthropicRefusal is the Anthropic adapter (10 §4). A 429's unified headers
// name what refused, by meaning (C5): each rejected window is spent, keyed by
// the window, or by the model where no reading carries it (an unmapped
// token). A 429 that rejects no window, or carries no unified headers at all,
// is a throttle; the client-shape 429 (#90) is wrong_member. The money
// refusals (10 §4, A-D6) are unpaid on the credits pot, which no reading sees
// (10 U6): usage credits out ("credit balance is too low", 400), billing
// (402), the user-set spend limit ("specified API usage limits", 400), a
// third-party app's "draw from your extra usage" (400), and the Console
// tier's spend cap (429 enforced_spend_limit_reached, no unified headers:
// money, not a throttle). Any other 400 is invalid.
func anthropicRefusal(resp *http.Response, model string, now time.Time) refusal {
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		b := bufferResp(resp)
		if shapeRejected429(resp, b) {
			return refusal{class: refWrongMember, detail: "client-shape rejection", body: b}
		}
		if strings.Contains(string(b), "enforced_spend_limit_reached") {
			return refusal{class: refUnpaid, limits: []refLimit{{name: potCredits, kind: limPot}},
				detail: refDetail(b, "enforced_spend_limit_reached"), body: b}
		}
		obs, ok := parseUnifiedRL(resp.Header, now, currentScopes())
		rej := obs.rejectedWindows()
		if !ok || len(rej) == 0 {
			return refusal{class: refThrottled, detail: "429 that named no rejected window", body: b}
		}
		r := refusal{class: refSpent, body: b}
		var labels []string
		for _, name := range rej {
			reset := time.Time{}
			for _, w := range obs.windows {
				if w.name == name {
					reset = w.reset
				}
			}
			if reset.IsZero() {
				reset = obs.reset
			}
			l := refLimit{name: name, kind: limWindow, resets: reset}
			if strings.HasPrefix(name, "unmapped:") {
				l = refLimit{name: modelLimit(model), kind: limModel, resets: reset}
			}
			if refHas(r.limits, l.name) {
				continue // a window and its claim can map to the one model record
			}
			r.limits = append(r.limits, l)
			labels = append(labels, windowLabel(name))
		}
		r.detail = "rate limit rejected (" + strings.Join(labels, ", ") + ")"
		return r
	case http.StatusPaymentRequired:
		b := bufferResp(resp)
		return refusal{class: refUnpaid, limits: []refLimit{{name: potCredits, kind: limPot}},
			detail: refDetail(b, "402 billing_error"), body: b}
	case http.StatusBadRequest:
		b := bufferResp(resp)
		if l := strings.ToLower(string(b)); anthropicMoney400(l) {
			return refusal{class: refUnpaid, limits: []refLimit{{name: potCredits, kind: limPot}},
				detail: refDetail(b, "400 money refusal"), body: b}
		}
		return refusal{class: refInvalid, detail: refDetail(b, "400"), body: b}
	}
	if resp.StatusCode >= 500 {
		return refusal{class: refDegraded}
	}
	return refusal{}
}

// anthropicMoney400 is a 400 body (lowered) that says money, not the request
// (10 §4, A-D6): the Console's credits out and its user-set spend limit
// ("specified API usage limits", "specified workspace API usage limits"), and
// a third-party app on OAuth drawing from usage credits that are off.
func anthropicMoney400(l string) bool {
	return strings.Contains(l, "credit balance is too low") ||
		(strings.Contains(l, "specified") && strings.Contains(l, "api usage limits")) ||
		strings.Contains(l, "draw from your extra usage")
}

// codexErrorBody is a codex refusal's error object, at `error` or `detail`
// (11 S9; the backend has used both).
type codexErrorBody struct {
	Type               string  `json:"type"`
	Code               string  `json:"code"`
	Message            string  `json:"message"`
	LimitWindowMinutes float64 `json:"limit_window_minutes"`
}

// codexMoneyCodes are OpenAI's billing codes (O-D10, 11 §4): money or a spend
// setting, never a time.
var codexMoneyCodes = map[string]bool{
	"insufficient_quota":                true,
	"credit_balance_exhausted":          true,
	"organization_spend_limit_exceeded": true,
	"project_spend_limit_exceeded":      true,
	"organization_usage_limit_exceeded": true,
}

// codexModelNotOffered is a codex 400 body that says the account may not use
// the model through Codex, in the backend's own words (11 §4).
func codexModelNotOffered(body []byte) bool {
	return strings.Contains(strings.ToLower(string(body)), "is not supported when using codex with a chatgpt account")
}

// codexBodyMessage is a codex error body's message wherever the backend put
// it: a bare `detail` string, or the message of `error` or `detail`.
func codexBodyMessage(body []byte) string {
	var bare struct {
		Detail string `json:"detail"`
	}
	if json.Unmarshal(body, &bare) == nil && bare.Detail != "" {
		return bare.Detail
	}
	var env struct {
		Error  *codexErrorBody `json:"error"`
		Detail *codexErrorBody `json:"detail"`
	}
	_ = json.Unmarshal(body, &env)
	switch {
	case env.Error != nil:
		return env.Error.Message
	case env.Detail != nil:
		return env.Detail.Message
	}
	return ""
}

// codexRefusal is the codex adapter (11 §4). A 429:
//   - usage_limit_reached is spent on the window its limit_window_minutes
//     names (the brain names a window by its length, so the record is on the
//     limit the reading carries), else the window its x-codex headers show at
//     100, else the model; the reset is the body's resets_at;
//   - usage_not_included is not_entitled for that model (no reading says);
//   - a billing code is unpaid on the credits pot, whose gate the codex
//     reading carries (the codex credits gate, attributed);
//   - anything else (slow_down, a bare rate limit) is a throttle, its
//     Retry-After honoured (O-D13).
//
// A 404 is the backend's, not the request's: codex's outage answers every
// model with an empty-body 404 (11 §4, OBSERVED, "Wait"), which is
// degraded (the health ladder; nothing to walk on where health does not
// govern the class, and the client may retry). A 404 whose body names a model
// says this backend serves no such model: wrong_member, walked with no record
// (11 §4 records no such shape, so nothing closes on it).
//
// A 400 saying the model "is not supported when using Codex with a ChatGPT
// account" is the account's, not the request's (11 §4, OBSERVED:
// a plan stopped offering a model it had served hours before, and
// the image door's walk handed the 400 to the client instead of the next member). No
// client names a codex model: a class member does, or the image door's host
// model. So it is not_entitled for the member's model, as usage_not_included
// is: the walk moves on, and the record holds until the owner's re-check.
//
// 401 and 403 are auth, which the wire re-auths once (tripWithReauth) and
// classify stops on; any other 4xx is invalid, with codex's error.code.
func codexRefusal(resp *http.Response, model string, now time.Time) refusal {
	sc := resp.StatusCode
	if sc >= 500 {
		return refusal{class: refDegraded}
	}
	if sc == http.StatusUnauthorized || sc == http.StatusForbidden {
		return refusal{}
	}
	b := bufferResp(resp)
	if sc == http.StatusNotFound {
		if strings.Contains(strings.ToLower(string(b)), "model") {
			return refusal{class: refWrongMember, detail: refDetail(b, "404"), body: b}
		}
		return refusal{class: refDegraded, detail: refDetail(b, "404 (codex outage)"), body: b}
	}
	if sc == http.StatusBadRequest && codexModelNotOffered(b) {
		return refusal{class: refNotEntitled, limits: []refLimit{{name: modelLimit(model), kind: limModel}},
			detail: refDetailText(codexBodyMessage(b), "400 model not offered to this account"), body: b}
	}
	var env struct {
		Error  *codexErrorBody `json:"error"`
		Detail *codexErrorBody `json:"detail"`
		codexErrorBody
	}
	_ = json.Unmarshal(b, &env) // an unreadable body is a throttle below
	e := env.codexErrorBody
	if env.Error != nil {
		e = *env.Error
	} else if env.Detail != nil {
		e = *env.Detail
	}
	if sc != http.StatusTooManyRequests {
		return refusal{class: refInvalid, detail: refDetail(b, http.StatusText(sc)), code: e.Code, body: b}
	}
	detail := e.Message
	if detail == "" {
		detail = e.Type
	}
	if detail == "" {
		detail = "429"
	}
	switch {
	case e.Type == "usage_limit_reached":
		reset := quotaResetAt(nil, b, now)
		if reset.IsZero() {
			reset = quotaResetAt(resp.Header, nil, now)
		}
		name := ""
		if e.LimitWindowMinutes > 0 {
			name, _ = codexWindowName(e.LimitWindowMinutes*60, map[string]checkWindow{})
		}
		if name == "" {
			if hr, ok := parseCodexHeaders(resp.Header, now); ok {
				for _, n := range []string{"seven_day", "five_hour"} {
					if w, has := hr.windows[n]; has && w.UsedPct != nil && *w.UsedPct >= 100 {
						name = n
						break
					}
				}
			}
		}
		l := refLimit{name: name, kind: limWindow, resets: reset}
		if name == "" {
			l = refLimit{name: modelLimit(model), kind: limModel, resets: reset}
		}
		return refusal{class: refSpent, limits: []refLimit{l}, detail: "usage_limit_reached: " + detail, body: b}
	case e.Type == "usage_not_included" || e.Code == "usage_not_included":
		return refusal{class: refNotEntitled, limits: []refLimit{{name: modelLimit(model), kind: limModel}},
			detail: "usage_not_included: " + detail, body: b}
	case codexMoneyCodes[e.Code] || codexMoneyCodes[e.Type]:
		code := e.Code
		if code == "" {
			code = e.Type
		}
		return refusal{class: refUnpaid, limits: []refLimit{{name: potCredits, kind: limPot, readable: true}},
			detail: code + ": " + detail, body: b}
	}
	return refusal{class: refThrottled, detail: detail, body: b}
}

// kimiRefusal is the Kimi adapter (12 §4): Kimi says what refused in words,
// with a 403 or a 429 for the same meaning, so its wordings are the
// classification and its status is not.
//   - the 5-hour and the weekly cap are spent on those windows;
//   - the billing-cycle (monthly) cap is spent on the monthly pool, which no
//     reading maps yet (K2): unattributed;
//   - credit and Extra Usage refusals are unpaid on its credits pot, which has
//     no API (12 §2): unattributed;
//   - "does not have access to Kimi Code" (an expired membership, K4) and
//     "unable to verify your membership" are not_entitled on the plan, which
//     the poll reads (a lapsed account reads no usage);
//   - "does not have access to <model>" and "supports only" are not_entitled
//     for that model;
//   - concurrency, burst and overload are throttles (K15), as is any other 429;
//   - any other 403 is not a refusal it can name, and any 400 is invalid.
//
// The reset each names is the one its body or Retry-After says (quotaResetAt).
func kimiRefusal(resp *http.Response, model string, now time.Time) refusal {
	sc := resp.StatusCode
	if sc >= 500 {
		return refusal{class: refDegraded}
	}
	if sc != http.StatusForbidden && sc != http.StatusTooManyRequests && sc != http.StatusBadRequest {
		return refusal{}
	}
	b := bufferResp(resp)
	detail := refDetail(b, http.StatusText(sc))
	l := strings.ToLower(detail + " " + string(b))
	reset := quotaResetAt(resp.Header, b, now)
	one := func(class string, lim refLimit) refusal {
		lim.resets = reset
		return refusal{class: class, limits: []refLimit{lim}, detail: detail, body: b}
	}
	switch {
	case strings.Contains(l, "5-hour usage limit"):
		return one(refSpent, refLimit{name: "five_hour", kind: limWindow})
	case strings.Contains(l, "weekly") && strings.Contains(l, "usage limit"):
		return one(refSpent, refLimit{name: "seven_day", kind: limWindow})
	case strings.Contains(l, "billing cycle") || strings.Contains(l, "monthly usage limit"):
		return one(refSpent, refLimit{name: "total_quota", kind: limPool})
	case strings.Contains(l, "credit balance is insufficient") || strings.Contains(l, "extra usage"):
		return one(refUnpaid, refLimit{name: potCredits, kind: limPot})
	case strings.Contains(l, "does not have access to kimi code") || strings.Contains(l, "verify your membership"):
		return one(refNotEntitled, refLimit{name: limitEntitlement, kind: limEntitlement, readable: true})
	case strings.Contains(l, "does not have access to") || strings.Contains(l, "supports only"):
		return one(refNotEntitled, refLimit{name: modelLimit(model), kind: limModel})
	case strings.Contains(l, "usage limit"):
		return one(refSpent, refLimit{name: modelLimit(model), kind: limModel})
	}
	switch sc {
	case http.StatusTooManyRequests:
		return refusal{class: refThrottled, detail: detail, body: b}
	case http.StatusBadRequest:
		return refusal{class: refInvalid, detail: detail, body: b}
	}
	return refusal{body: b}
}

func refHas(ls []refLimit, name string) bool {
	for _, l := range ls {
		if l.name == name {
			return true
		}
	}
	return false
}
