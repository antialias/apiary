package main

// ---- per-model registry (#36) ----
//
// The registry is the single source of truth for the Anthropic ids the proxy
// ADVERTISES beside its class rows (GET /v1/models + the client picker cache):
// what the row is called and what context window it declares. It exists because
// Claude Code infers a model's context window from its NAME: ids must begin
// with "claude"/"anthropic" to appear in the picker at all, that prefix locks
// them out of CLAUDE_CODE_MAX_CONTEXT_TOKENS, and the ONLY window lever left is
// the literal substring "[1m]" (a bare client-side regex => 1M; anything else
// => a silent 200k default). See spikes/model-registry/FINDINGS.md. Two
// invariants fall out:
//
//   - the "[1m]" suffix is RENDERER-OWNED: it appears iff the declared window is
//     >= 1M, and a config entry may never author it by hand (overclaiming turns
//     soft failure — early compaction — into a hard prompt_too_long);
//   - a window is DECLARED, never inferred.
//
// Config: SWITCH_MODELS_FILE (default ~/.claude-switch-proxy/models.json, set in
// main() ONLY so tests stay hermetic; "off"/"0" disables). A JSON array of
// entries:
//
//	[{"name": "opus-5-5", "display": "Claude Opus 5.5 (1M)", "type": "anthropic",
//	  "context_window": 1000000}]
//
// No file => an empty registry ("legacy"): only the class rows advertise.
//
// There is no provider row. The local runtime's claude-local-<tag> rows went
// with #135 (the local runtime is now a member of the `local` class). The four "named"
// rows (kimi-k3, gpt-6.1-sol, gpt-6-luna, gpt-6-astra) and their policy-free
// direct path were deleted in quota-model C2 (§2.2 row 10, agreed decision 4):
// a hosted provider's model is
// reached only as a member of a workload class, where the brain's route, pay
// list and dial check apply. The member's token ratio and its calibration
// provenance come from the brain's /preferred with the member (class.go).
// A stale picker id from those rows is refused typed (hostedProviderNamespace
// in main.go), never forwarded to api.anthropic.com.
//
// Validation is per-entry drop-with-logged-reason for malformed entries. Class
// rows (#27) advertise a synthetic id the brain resolves per-session via
// /class-bind; since #93 (B2) they DERIVE from the brain's /preferred instead
// of this file (see classrows.go) — a class-shaped entry still present in a
// models.json is logged and ignored (grace, never a double-advertise). A
// removed class is QUARANTINED as a tombstone (recognized, NOT advertised,
// answered with a typed 503), which closes the silent-404 trap: a stale
// picker-cache id fails loud instead of leaking to api.anthropic.com.
//
// Anthropic rows (#96) are PURE ADVERTISEMENT: name + context_window, no
// provider, no upstream_model, no ratio. Claude Code's gateway model discovery
// treats /v1/models as the whole world and silently substitutes another model
// for a subagent that asked for an id missing from it, so real Anthropic ids
// have to be listed — while still routing by LANE, byte-identically to an id
// nobody advertised. An anthropic name must carry a DIGIT: a digit-free
// rendered id would land in the class-shaped refusal namespace (classrows.go
// rule 3) and be refused instead of routed.
//
// A malformed FILE never kills the process (a supervisor's restart would crash-loop
// the ANTHROPIC_BASE_URL listener): log loud, fall back to legacy, and surface
// registry.source="legacy" + registry.error on /_switch/status.

import (
	"encoding/json"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type registryEntry struct {
	Name          string `json:"name"`                     // rendered id = "claude-"+Name (+"[1m]" iff ContextWindow >= oneMegaMarkFloor)
	Display       string `json:"display"`                  // picker row text; defaulted when empty
	Type          string `json:"type"`                     // "class" | "anthropic" (advertisement only)
	Provider      string `json:"provider,omitempty"`       // never valid: an anthropic row declares no routing
	UpstreamModel string `json:"upstream_model,omitempty"` // never valid: an anthropic row declares no routing
	Class         string `json:"class,omitempty"`          // class only: brain class name
	ContextWindow int    `json:"context_window"`           // declared, never inferred; 0 = unknown (legacy only)
	Guidance      string `json:"guidance,omitempty"`       // class only: brain-owned purpose
	FirstChoice   string `json:"first_choice,omitempty"`   // class only: lowest-priority member's display name
	DirectModel   string `json:"direct_model,omitempty"`   // class only: lowest-priority ANTHROPIC member's model id; empty when every member is a provider

	// AnthropicTokenRatio is never valid on a row: it is read only so that a
	// stray one is refused (validateRegistry) rather than silently ignored. A
	// class provider member's ratio comes from the brain (classMemberPref).
	AnthropicTokenRatio float64 `json:"anthropic_token_ratio,omitempty"`

	disabledReason string // "" = routable + advertisable; else quarantined (recognized -> typed 503)

	// pickerHidden marks a class row that belongs to a consumer which is not a
	// Claude Code client (#95 request_shape). It stays fully routable — other consumers
	// reach their class exactly as before — but it is kept out
	// of /v1/models and never marked "[1m]": the suffix is picker metadata, and a
	// row no picker offers has no window to communicate.
	pickerHidden bool

	// Modality is what a class row serves (§2.9, C4): "" for chat, or
	// modalityImage for the image door's class, which the chat door refuses
	// and no picker lists. From the brain's class request_kind; persisted with
	// the last-good rows, so a boot without the brain keeps it.
	Modality string `json:"modality,omitempty"`
}

const modalityImage = "image"

// modalityName is a row's modality in a refusal.
func (e *registryEntry) modalityName() string {
	if e.Modality == "" {
		return "chat"
	}
	return e.Modality
}

var (
	// modelsFilePath is resolved from SWITCH_MODELS_FILE in main() ONLY. The
	// package default "" = legacy synthesis, which keeps every test that calls
	// buildHandler() hermetic (never reading a production models.json).
	modelsFilePath string
	registry       []registryEntry // rebuilt by buildHandler, like providers
	registrySource string          // "file" | "legacy"
	registryErr    string          // non-"" = file present but unusable (fell back to legacy)
)

// oneMegaMarkFloor is the window at or above which renderedID()
// appends "[1m]". Resolved from SWITCH_1M_MARK_FLOOR in main() ONLY; the package
// default 1M is the literal rule this started as, so every test stays hermetic.
//
// The suffix is the client's ONLY window signal: a claude-* id it cannot parse
// is assumed to be 200k, and it auto-compacts at that ceiling no matter how big
// the upstream window really is. Lowering this floor therefore re-reads the
// suffix as "at least this many client-unit tokens", which OVER-DECLARES the
// window for every marked row below 1M. That is safe only while the client's own
// autoCompactWindow caps the session below the smallest marked row's real window.
// Raise this floor freely; lowering it past a real window is a hard upstream
// failure, not a degradation.
//
// Every row's window is already in client units: anthropic rows share the
// client's tokenizer, and a class row's window is the brain's, derived from
// each member's own window and ratio.
var oneMegaMarkFloor = 1_000_000

// ---- what an Anthropic row IS (launch-sets §15.3) ----
//
// The rendered ids are not uniform (claude-opus-5, claude-opus-5[1m],
// claude-fable-5-1, claude-haiku-4-5-20251001), so NO consumer may parse one.
// The proxy states the facts instead, derived from the registry entry's own
// name in this ONE place: family + version, which is what lets a consumer rank
// "latest Opus" without knowing a thing about Anthropic's naming.

// anthropicFamilies is the closed set §15.3 names. A name outside it yields no
// family: an unrecognised family is a registry fact we do not have, and
// inventing one would let a consumer rank a row it cannot understand.
var anthropicFamilies = map[string]bool{"opus": true, "sonnet": true, "fable": true, "haiku": true}

// anthropicDateSegment matches the trailing snapshot date in a name like
// haiku-4-5-20251001. A date is NOT part of the version (§15.3) — two snapshots
// of 4.5 are the same version — and it ends the version at that point.
var anthropicDateSegment = regexp.MustCompile(`^\d{6,}$`)

// anthropicNameParts splits an anthropic entry's NAME (never its rendered id)
// into family and dotted version. ok=false means the name does not describe a
// known family with at least one version number, i.e. the row cannot be ranked;
// the test over the real models.json is what keeps that out of a release.
//
//	opus-5             -> opus,   "5"
//	fable-5-1          -> fable,  "5.1"
//	haiku-4-5-20251001 -> haiku,  "4.5"
func anthropicNameParts(name string) (family, version string, ok bool) {
	parts := strings.Split(name, "-")
	if len(parts) < 2 || !anthropicFamilies[parts[0]] {
		return "", "", false
	}
	nums := []string{}
	for _, p := range parts[1:] {
		if anthropicDateSegment.MatchString(p) {
			break // a snapshot date ends the version
		}
		if _, err := strconv.Atoi(p); err != nil {
			break // anything non-numeric ends it too ("-latest", "-preview")
		}
		nums = append(nums, p)
	}
	if len(nums) == 0 {
		return "", "", false
	}
	return parts[0], strings.Join(nums, "."), true
}

// renderedID is the advertised/picker id. The "[1m]" suffix is the client's only
// window signal and is appended here and nowhere else.
func (e *registryEntry) renderedID() string {
	id := "claude-" + e.Name
	if e.pickerHidden {
		// Never marked: an unadvertised row's id is a routing key its consumer
		// hard-codes, and the marker would move under it every time the brain
		// re-derives the window (one went 200000 -> 419672).
		return id
	}
	if e.ContextWindow >= oneMegaMarkFloor {
		id += "[1m]"
	}
	return id
}

// loadRegistry returns the registry, its source ("file"|"legacy"), and a
// file-level error string ("" when clean). Legacy is the empty registry.
func loadRegistry(path string) ([]registryEntry, string, string) {
	if path == "" {
		return []registryEntry{}, "legacy", ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []registryEntry{}, "legacy", "" // absent file is a normal mode, not an error
		}
		log.Printf("registry: cannot read %s: %v — advertising no anthropic rows", path, err)
		return []registryEntry{}, "legacy", err.Error()
	}
	var entries []registryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		log.Printf("registry: bad JSON in %s: %v — advertising no anthropic rows", path, err)
		return []registryEntry{}, "legacy", err.Error()
	}
	return validateRegistry(entries), "file", ""
}

// validateRegistry hard-drops malformed entries (logged reason each). First
// rendered id wins a collision.
func validateRegistry(raw []registryEntry) []registryEntry {
	drop := func(e registryEntry, why string) {
		log.Printf("registry: dropping entry %q: %s", e.Name, why)
	}
	seen := map[string]bool{}
	out := []registryEntry{}
	for _, e := range raw {
		if e.Name == "" {
			drop(e, "missing name")
			continue
		}
		if strings.Contains(strings.ToLower(e.Name), "[1m]") {
			drop(e, `"[1m]" is renderer-owned (appended iff context_window >= 1M), never config-authored`)
			continue
		}
		switch e.Type {
		case "class":
			// #93 (B2): class rows derive from the brain's /preferred (see
			// classrows.go). GRACE for a not-yet-migrated file: log and ignore —
			// registering it here too would double-advertise the id.
			log.Printf("registry: ignoring class entry %q — class picker rows derive from the brain's /preferred (#93); remove it from models.json", e.Name)
			continue
		case "anthropic":
			// #96: advertisement only. Nothing here may be routable state — the
			// id keeps riding the lane path — so the row declares a window and a
			// name and nothing else, and can never be quarantined.
			if e.ContextWindow <= 0 {
				drop(e, "missing context_window (declared, never inferred)")
				continue
			}
			if !strings.ContainsAny(e.Name, "0123456789") {
				drop(e, "an anthropic row's name must contain a digit: a digit-free rendered id sits in "+
					"the class-shaped refusal namespace (classrows.go rule 3) and would be refused with a "+
					"typed 503 instead of routed to a lane")
				continue
			}
			if e.Provider != "" || e.UpstreamModel != "" || e.Class != "" || e.AnthropicTokenRatio != 0 || e.Modality != "" {
				// Strict: routing is declared, never inferred, and an anthropic
				// row declares NO routing. A stray field
				// would read as authority the type does not have — a provider that
				// never projects, an upstream id nothing rewrites to, a class nothing
				// binds, a ratio nothing meters. Refuse the row instead of silently
				// advertising it with the field ignored.
				drop(e, "an anthropic row is advertisement only: provider/upstream_model/class/"+
					"anthropic_token_ratio/modality must be absent (the id routes by lane, so none of them can take effect)")
				continue
			}
			if _, _, ok := anthropicNameParts(e.Name); !ok {
				// WARN, never drop: the id still routes by lane and a consumer
				// that only needs a picker row is unharmed, so refusing the
				// model outright would be the bigger outage. What it costs is
				// rankability (§15.3's "latest Opus"), and the test over the
				// real models.json is the gate that keeps it out of a release.
				log.Printf("registry: anthropic entry %q yields no family/version (§15.3) — it will "+
					"advertise, but no consumer can rank it; name it <family>-<major>[-<minor>][-<date>] "+
					"with family in opus|sonnet|fable|haiku", e.Name)
			}
			if e.Display == "" {
				e.Display = e.renderedID()
			}
		default:
			drop(e, "type must be class|anthropic")
			continue
		}
		id := e.renderedID()
		if seen[id] {
			drop(e, "rendered id "+id+" already taken (first wins)")
			continue
		}
		seen[id] = true
		out = append(out, e)
	}
	return out
}

// registryClassRows, matchRegistryClass, and the class-shaped refusal now live
// in classrows.go (#93): class rows derive from /preferred, not this file.

// registryAnthropicRows returns the picker rows for "anthropic" entries (#96),
// mirroring registryClassRows. There is no upstreamID: nothing rewrites the
// model id and no provider claims it, so the request falls through to lane
// routing exactly as it did before the row was advertised.
func registryAnthropicRows() []advertisedModel {
	out := []advertisedModel{}
	for i := range registry {
		e := &registry[i]
		if e.Type == "anthropic" && e.disabledReason == "" {
			family, version, _ := anthropicNameParts(e.Name)
			out = append(out, advertisedModel{pickerID: e.renderedID(), display: e.Display,
				// The one kind where effort is unconditionally true: the row is
				// a real Anthropic id, nothing claims it, and the body reaches
				// api.anthropic.com byte-for-byte (see advertisedModel).
				kind: kindAnthropic, effort: true,
				family: family, version: version, contextWindow: e.ContextWindow})
		}
	}
	return out
}

// advertisableConfigured reports whether anything can ever be advertised —
// the boot-time predicate for the picker-cache decision (sync loop vs remove).
// Deliberately config-shaped, not moment-shaped: a momentarily-empty
// advertised set never deletes the client's picker cache. A configured brain
// counts (#93): class rows derive from its
// /preferred, so they WILL be advertised when it answers — and rows already
// boot-loaded from the last-good snapshot count brain-down too.
//
// An anthropic-only registry (#96) also counts, and the picker cache it syncs is
// harmless: those rows carry REAL Anthropic ids, which resolve whether or not
// this proxy is in the path. The "rows would 404 at Anthropic" removal rationale
// at the call site applies only to provider-owned ids nobody but this proxy serves.
func advertisableConfigured() bool {
	for _, e := range registry {
		if e.disabledReason == "" {
			return true
		}
	}
	if brainBase != "" {
		return true
	}
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	for i := range classRowsSet {
		if classRowsSet[i].disabledReason == "" {
			return true
		}
	}
	return false
}

// registryStatus is the /_switch/status view: full entries including quarantine
// reasons, with the brain-derived class rows folded in (#93)
// so the entries list keeps its pre-B2 shape — status consumers still see class
// rows as registry entries, tombstones as enabled=false/"removed". The additive
// class_source key says where those rows came from.
func registryStatus() map[string]any {
	classRows, classSource := classRowsSnapshot()
	entries := make([]map[string]any, 0, len(registry)+len(classRows))
	add := func(e *registryEntry) {
		entries = append(entries, map[string]any{
			"name":            e.Name,
			"id":              e.renderedID(),
			"type":            e.Type,
			"class":           e.Class,
			"context_window":  e.ContextWindow,
			"enabled":         e.disabledReason == "",
			"disabled_reason": e.disabledReason,
		})
	}
	for i := range registry {
		add(&registry[i])
	}
	for i := range classRows {
		add(&classRows[i])
	}
	if classSource == "" {
		classSource = "none"
	}
	return map[string]any{
		"source":       registrySource,
		"error":        registryErr,
		"class_source": classSource, // "preferred" (live) | "last-good" (boot snapshot) | "none"
		"entries":      entries,
	}
}
