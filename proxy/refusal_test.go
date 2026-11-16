package main

// The refusal taxonomy (quota-model 50 §2.6, C7): each provider's refusals,
// from the responses the provider files record (10 §4, 11 §4, 12 §4),
// classified by its adapter. The class alone decides whether the walk moves
// on and what is recorded; the status never does.

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Recorded refusals. OBS = seen in live traffic, DOC = the provider's docs or
// source, in the provider's own envelope.
const (
	// Kimi (12 §4).
	kimiNotEntitled403  = `{"type":"error","error":{"type":"permission_error","message":"Your current subscription does not have access to Kimi Code right now"}}` // OBS (K4)
	kimiVerifyMember403 = `{"type":"error","error":{"type":"permission_error","message":"We're unable to verify your membership benefits at this time."}}`         // DOC
	kimiWeekly429       = `{"type":"error","error":{"type":"rate_limit_error","message":"You've reached your weekly (7-day) usage limit."}}`                       // DOC (K5)
	kimiCredit429       = `{"type":"error","error":{"type":"rate_limit_error","message":"Your credit balance is insufficient."}}`                                  // DOC (K6)
	kimiExtraUsage429   = `{"type":"error","error":{"type":"rate_limit_error","message":"Your organization's Extra Usage balance has run out."}}`                  // DOC (K6)
	kimiExtraLimit429   = `{"type":"error","error":{"type":"rate_limit_error","message":"You've reached the Extra Usage limit set for your account by your organization."}}`
	kimiConcurrent429   = `{"type":"error","error":{"type":"rate_limit_error","message":"You've reached your concurrent request limit."}}` // DOC (K15)
	kimiModel403        = `{"type":"error","error":{"type":"permission_error","message":"Your current plan does not have access to kimi-k3-max"}}`
	kimiAuth403         = `{"type":"error","error":{"type":"permission_error","message":"invalid api key"}}`

	// Codex (11 §4). S9 is OBS (astra); the rest DOC
	// (codex api_bridge.rs, the OpenAI error-code guide).
	codexS9At               = 1793566800
	codexS9                 = `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1793566800,"eligible_promo":null,"limit_window_minutes":10080,"resets_in_seconds":259200}}`
	codexS9FiveHour         = `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":1793566800,"limit_window_minutes":300}}`
	codexNotIncluded429     = `{"error":{"type":"usage_not_included","message":"This model is not included in your plan"}}`
	codexCreditExhausted429 = `{"error":{"type":"insufficient_quota","code":"credit_balance_exhausted","message":"You exceeded your current quota, please check your plan and billing details."}}`
	codexOrgSpend429        = `{"error":{"type":"insufficient_quota","code":"organization_spend_limit_exceeded","message":"Organization spend limit exceeded."}}`
	codexProjectSpend429    = `{"error":{"code":"project_spend_limit_exceeded","message":"Project spend limit exceeded."}}`
	codexOrgUsage429        = `{"error":{"code":"organization_usage_limit_exceeded","message":"Organization usage limit exceeded."}}`
	codexInsufficient429    = `{"error":{"type":"insufficient_quota","message":"You exceeded your current quota."}}`
	codexSlowDown429        = `{"error":{"type":"rate_limit_error","code":"slow_down","message":"Please slow down."}}` // DOC (O-D13)

	// Anthropic (10 §4).
	anthCreditTooLow400 = `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}` // DOC (A-D6)
	anthBilling402      = `{"type":"error","error":{"type":"billing_error","message":"There is an issue with your account's billing."}}`                                                                                   // DOC (A-D6)
	anthInvalid400      = `{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens: Field required"}}`
	// The rest of A-D6, in 10 §4's words: the Console's user-set spend limit
	// (DOC), a third-party app on OAuth drawing on usage credits (OBS, memory),
	// and the Console tier's spend cap, a 429 with no unified headers (DOC).
	anthSpendLimit400     = `{"type":"error","error":{"type":"invalid_request_error","message":"You have reached your specified API usage limits. You will regain access on 2026-12-01 at 00:00 UTC."}}`
	anthWorkspaceSpend400 = `{"type":"error","error":{"type":"invalid_request_error","message":"You have reached your specified workspace API usage limits. You will regain access on 2026-12-01 at 00:00 UTC."}}`
	anthThirdParty400     = `{"type":"error","error":{"type":"invalid_request_error","message":"Third-party apps now draw from your extra usage, not your plan limits. Add more at claude.ai/settings/usage and keep going."}}`
	anthSpendCap429       = `{"type":"error","error":{"type":"rate_limit_error","message":"You have reached your API usage limits. Your access will resume on 2026-12-01 at 00:00 UTC.","details":{"error_code":"enforced_spend_limit_reached"}}}`

	// Codex's outage (11 §4, OBS): an empty-body 404 for every
	// model. A 404 naming the model is no recorded shape; its body here is
	// the OpenAI API's model_not_found wording.
	codexModel404   = `{"error":{"type":"invalid_request_error","code":"model_not_found","message":"The model 'gpt-5.6' does not exist or you do not have access to it."}}`
	codexInvalid400 = `{"error":{"type":"invalid_request_error","code":"invalid_value","message":"Invalid size '7x7'. Supported sizes are 1024x1024, 1024x1536, 1536x1024 and auto."}}`

	// A model the ChatGPT plan no longer offers (11 §4, OBS, after
	// a plan lapsed). The body is not logged: the client read the
	// sentence through error.message, and Codex's CLI reports the same
	// sentence as a bare detail string, so both shapes are pinned.
	codexModelNotOffered400       = `{"error":{"type":"invalid_request_error","message":"The 'gpt-6.1-sol' model is not supported when using Codex with a ChatGPT account."}}`
	codexModelNotOfferedDetail400 = `{"detail":"The 'gpt-6.1-sol' model is not supported when using Codex with a ChatGPT account."}`
)

// refLimits is "name/kind[/readable]" per limit, sorted.
func refLimits(r refusal) string {
	var out []string
	for _, l := range r.limits {
		s := l.name + "/" + l.kind
		if l.readable {
			s += "/readable"
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func TestRefusalClassesFromRecordedResponses(t *testing.T) {
	withTestProviders(t)
	now := time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	reset5h := now.Add(2 * time.Hour)
	anth5h := map[string]string{
		"anthropic-ratelimit-unified-status":               "rejected",
		"anthropic-ratelimit-unified-representative-claim": "five_hour",
		"anthropic-ratelimit-unified-5h-status":            "rejected",
		"anthropic-ratelimit-unified-5h-reset":             strconv.FormatInt(reset5h.Unix(), 10),
		"anthropic-ratelimit-unified-7d-status":            "allowed",
		"anthropic-ratelimit-unified-7d-reset":             strconv.FormatInt(now.Add(72*time.Hour).Unix(), 10),
		"anthropic-ratelimit-unified-5h-utilization":       "1.0",
	}
	codexWeek := map[string]string{"x-codex-secondary-used-percent": "100", "x-codex-secondary-window-minutes": "10080",
		"x-codex-primary-used-percent": "40", "x-codex-primary-window-minutes": "300"}
	for _, c := range []struct {
		name   string
		m      classMemberPref
		status int
		hdr    map[string]string
		body   string
		class  string
		limits string
		walks  bool
	}{
		// Anthropic.
		{"anthropic 5h rejected", memAnth, 429, anth5h, `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`,
			refSpent, "five_hour/window", true},
		{"anthropic 429 with no unified headers", memAnth, 429, nil, `{"type":"error","error":{"type":"rate_limit_error","message":"Number of request tokens has exceeded your per-minute rate limit"}}`,
			refThrottled, "", true},
		{"anthropic client-shape 429", memAnth, 429, nil, shape429Body, refWrongMember, "", true},
		{"anthropic credit balance too low (400)", memAnth, 400, nil, anthCreditTooLow400, refUnpaid, "credits/pot", true},
		{"anthropic billing_error (402)", memAnth, 402, nil, anthBilling402, refUnpaid, "credits/pot", true},
		{"anthropic specified API usage limits (400)", memAnth, 400, nil, anthSpendLimit400, refUnpaid, "credits/pot", true},
		{"anthropic specified workspace API usage limits (400)", memAnth, 400, nil, anthWorkspaceSpend400, refUnpaid, "credits/pot", true},
		{"anthropic third-party draws from extra usage (400)", memAnth, 400, nil, anthThirdParty400, refUnpaid, "credits/pot", true},
		{"anthropic Console spend cap (429 enforced_spend_limit_reached)", memAnth, 429, nil, anthSpendCap429, refUnpaid, "credits/pot", true},
		{"anthropic other 400", memAnth, 400, nil, anthInvalid400, refInvalid, "", false},
		{"anthropic 529", memAnth, 529, nil, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, refDegraded, "", false},
		{"anthropic 401 has no class", memAnth, 401, nil, `{}`, "", "", false},

		// Codex.
		{"codex S9 weekly", memGpt, 429, codexWeek, codexS9, refSpent, "seven_day/window", true},
		{"codex S9 by limit_window_minutes alone", memGpt, 429, nil, codexS9FiveHour, refSpent, "five_hour/window", true},
		{"codex usage_limit_reached naming no window", memGpt, 429, nil, `{"detail":{"type":"usage_limit_reached"}}`, refSpent, "model:gpt-5.6/model", true},
		{"codex usage_not_included", memGpt, 429, nil, codexNotIncluded429, refNotEntitled, "model:gpt-5.6/model", true},
		{"codex credit_balance_exhausted", memGpt, 429, nil, codexCreditExhausted429, refUnpaid, "credits/pot/readable", true},
		{"codex organization_spend_limit_exceeded", memGpt, 429, nil, codexOrgSpend429, refUnpaid, "credits/pot/readable", true},
		{"codex project_spend_limit_exceeded", memGpt, 429, nil, codexProjectSpend429, refUnpaid, "credits/pot/readable", true},
		{"codex organization_usage_limit_exceeded", memGpt, 429, nil, codexOrgUsage429, refUnpaid, "credits/pot/readable", true},
		{"codex insufficient_quota", memGpt, 429, nil, codexInsufficient429, refUnpaid, "credits/pot/readable", true},
		{"codex slow_down", memGpt, 429, map[string]string{"Retry-After": "7"}, codexSlowDown429, refThrottled, "", true},
		{"codex bare 429", memGpt, 429, nil, `{}`, refThrottled, "", true},
		{"codex 403 is auth", memGpt, 403, nil, kimiAuth403, "", "", false},
		{"codex 400", memGpt, 400, nil, `{"error":{"message":"bad"}}`, refInvalid, "", false},
		{"codex 400: model not offered to a ChatGPT account", memGpt, 400, nil, codexModelNotOffered400, refNotEntitled, "model:gpt-5.6/model", true},
		{"codex 400: model not offered, as a bare detail", memGpt, 400, nil, codexModelNotOfferedDetail400, refNotEntitled, "model:gpt-5.6/model", true},
		{"codex outage: empty-body 404", memGpt, 404, nil, ``, refDegraded, "", false},
		{"codex 404 naming the model", memGpt, 404, nil, codexModel404, refWrongMember, "", true},
		{"codex 503", memGpt, 503, nil, `{"error":{"code":"server_is_overloaded"}}`, refDegraded, "", false},

		// Kimi: the wording decides, 403 or 429 alike.
		{"kimi 5-hour 403", memKimi, 403, nil, kimi403FiveHour, refSpent, "five_hour/window", true},
		{"kimi 5-hour as a 429", memKimi, 429, nil, kimi403FiveHour, refSpent, "five_hour/window", true},
		{"kimi weekly 429", memKimi, 429, nil, kimiWeekly429, refSpent, "seven_day/window", true},
		{"kimi billing cycle 403", memKimi, 403, nil, kimi403Cycle, refSpent, "total_quota/pool", true},
		{"kimi credit balance insufficient", memKimi, 429, nil, kimiCredit429, refUnpaid, "credits/pot", true},
		{"kimi Extra Usage run out", memKimi, 429, nil, kimiExtraUsage429, refUnpaid, "credits/pot", true},
		{"kimi Extra Usage limit", memKimi, 403, nil, kimiExtraLimit429, refUnpaid, "credits/pot", true},
		{"kimi no access to Kimi Code", memKimi, 403, nil, kimiNotEntitled403, refNotEntitled, "entitlement/entitlement/readable", true},
		{"kimi unable to verify membership", memKimi, 403, nil, kimiVerifyMember403, refNotEntitled, "entitlement/entitlement/readable", true},
		{"kimi model not in plan", memKimi, 403, nil, kimiModel403, refNotEntitled, "model:kimi-k3/model", true},
		{"kimi concurrency", memKimi, 429, nil, kimiConcurrent429, refThrottled, "", true},
		{"kimi auth 403 has no class", memKimi, 403, nil, kimiAuth403, "", "", false},
		{"kimi 400", memKimi, 400, nil, `{"error":{"message":"bad"}}`, refInvalid, "", false},
		{"kimi 500", memKimi, 500, nil, ``, refDegraded, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := rungRefusal(c.m, hResp(c.status, c.hdr, c.body))
			if r.class != c.class || refLimits(r) != c.limits || r.walks() != c.walks {
				t.Fatalf("got class %q limits %q walks %v, want %q %q %v", r.class, refLimits(r), r.walks(), c.class, c.limits, c.walks)
			}
			if r.records() != (c.walks && c.class != refWrongMember) {
				t.Fatalf("records() = %v for class %q", r.records(), r.class)
			}
		})
	}
	// An invalid refusal carries the upstream's own code and words, for the
	// client; a refusal that sent no code carries none.
	if r := rungRefusal(memGpt, hResp(400, nil, codexInvalid400)); r.code != "invalid_value" ||
		!strings.HasPrefix(r.detail, "Invalid size '7x7'") {
		t.Fatalf("codex invalid 400: code %q detail %q", r.code, r.detail)
	}
	if r := rungRefusal(memGpt, hResp(400, nil, `{"error":{"message":"bad"}}`)); r.code != "" {
		t.Fatalf("a 400 with no code carries code %q", r.code)
	}
	// A model the plan no longer offers carries ChatGPT's own sentence, from
	// either shape, so the record and the brain's ask say what refused.
	for _, body := range []string{codexModelNotOffered400, codexModelNotOfferedDetail400} {
		if r := rungRefusal(memGpt, hResp(400, nil, body)); !strings.HasPrefix(r.detail, "The 'gpt-6.1-sol' model is not supported") {
			t.Fatalf("model-not-offered detail %q from %s", r.detail, body)
		}
	}
	// The outage 404 is degraded at the head: row 7's detour, its own cause.
	if o := classify(memGpt, context.Background(), hResp(404, nil, ``), nil); o.row != "7" ||
		o.cause != "outage_404" || o.ladder != ladderDetour || o.sig != sigDegraded {
		t.Fatalf("codex outage 404 classified %+v, want row 7 outage_404 detour", o)
	}
	// A 2xx is no refusal, and its body is never read.
	ok := hResp(200, nil, `{"stream":"untouched"}`)
	if r := rungRefusal(memGpt, ok); r.class != "" || r.body != nil {
		t.Fatalf("a 200 classified as %+v", r)
	}
}

// The resets a refusal names: codex's resets_at, Anthropic's per-window reset,
// Kimi's Retry-After; none when nothing says.
func TestRefusalNamesItsReset(t *testing.T) {
	withTestProviders(t)
	now := time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	r := rungRefusal(memGpt, hResp(429, nil, codexS9))
	if !r.limits[0].resets.Equal(time.Unix(codexS9At, 0)) {
		t.Fatalf("codex S9 reset = %v, want resets_at", r.limits[0].resets)
	}
	r = rungRefusal(memKimi, hResp(429, map[string]string{"Retry-After": "600"}, kimiWeekly429))
	if !r.limits[0].resets.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("kimi reset = %v, want Retry-After", r.limits[0].resets)
	}
	if r = rungRefusal(memKimi, hResp(403, nil, kimi403Cycle)); !r.limits[0].resets.IsZero() {
		t.Fatalf("the billing-cycle 403 names no reset (K7), got %v", r.limits[0].resets)
	}
	if !strings.Contains(r.detail, "billing cycle") {
		t.Fatalf("detail %q should quote the upstream", r.detail)
	}
}
