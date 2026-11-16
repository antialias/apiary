package main

// ---- brain-derived class picker rows (#93 / B2) ----
//
// Class picker rows (claude-<class-id>, display, window, the renderer-owned
// "[1m]" suffix) DERIVE from the brain's /preferred classes payload instead of
// models.json: the brain owns the class documents, so the proxy advertising a
// locally-declared copy was a divergence waiting to be surfaced. models.json is
// reduced to the advertisement-only Anthropic rows (#96).
//
// Three requirements are load-bearing (ui-revamp.md §6), because quarantine only
// matches ids PRESENT in the registry, client picker caches have NO TTL, and a
// removed class id would otherwise fall through the guards onto the lane path
// and be forwarded to api.anthropic.com — the exact trap the typed-503
// quarantine exists to close. The standing invariant "advertising must work
// brain-down" also forbids depending on a live /preferred fetch:
//
//  1. LAST-GOOD PERSISTENCE: derived rows are persisted to a proxy-side state
//     file (SWITCH_CLASS_STATE_FILE, default ~/.claude-switch-proxy/
//     class-rows.json — same atomic tmp+rename pattern as the client picker
//     cache) and served from it at cold start / brain-down; a live /preferred
//     refresh replaces them. Startup never blocks on the brain.
//  2. TOMBSTONES: a class id that disappears from a fresh /preferred becomes a
//     tombstoned entry (disabledReason "removed") so a stale picker cache hits
//     the existing typed 503, not an upstream 404. Tombstones persist in the
//     last-good snapshot.
//  3. GENERALIZED REFUSAL: any id matching the class SHAPE that is neither
//     active nor tombstoned is refused with a typed 503 — never forwarded
//     upstream. This generalizes the old claude-delegate-* reserved prefix and
//     closes the "class created+removed while proxy state was lost" hole.
//
// Rule 3 only closes that hole if the ADVERTISE-SET IS A SUBSET OF THE
// REFUSABLE-SET: the brain's class-id charset is wider than the class shape
// (digits are legal there — ^[a-z][a-z0-9-]*$), and a digit-carrying id like
// "claude-k2", once advertised and later removed with the snapshot lost, is
// indistinguishable from a real Anthropic model id — it would ride the lane
// path to api.anthropic.com. So classRowGuard refuses to ADMIT any row whose
// rendered id the shape rule could not refuse later: never advertise what
// can't be refused. Enforced on both entry paths (live derive AND the
// last-good loader, which must also survive a hand-edited state file).
//
// Removed-class BINDINGS keep flowing into the brain's typed-503 refusal on
// /class-bind exactly as before — these rules cover the advertising/recognition
// side the brain never sees.
//
// KNOWN MIGRATION WINDOW (accepted, one boot wide): the very first boot on the
// upgraded binary has no snapshot yet, and models.json class entries are
// grace-ignored — so brain-down at that one boot advertises no class rows and
// answers class ids with the rule-3 typed 503 until the first successful
// /preferred document (the brain stream connects at startup and retries
// with a 1 s to 30 s backoff). Typed and loud, never
// an upstream leak; class ROUTING would be equally dead brain-down with a cold
// bind cache, and clients' no-TTL picker caches keep showing their cached rows
// anyway. Every later cold boot serves last-good.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// classRemovedReason is the tombstone's disabledReason (spec-pinned: "removed").
const classRemovedReason = "removed"

var (
	// classStateFilePath is resolved from SWITCH_CLASS_STATE_FILE in main()
	// ONLY. The package default "" disables persistence, which keeps every test
	// hermetic (never reading or writing a production class-rows.json) — the
	// same contract as modelsFilePath.
	classStateFilePath string

	classRowsMu     sync.RWMutex
	classRowsSet    []registryEntry // type "class" only; disabledReason==classRemovedReason ⇒ tombstone
	classRowsSource string          // "" = none yet | "last-good" (boot file) | "preferred" (live brain)
)

// classShapedRe is the workload-class namespace: "claude-" + digit-free
// lowercase hyphenated words, optionally with the renderer-owned "[1m]" suffix.
// Every real Anthropic model id carries a digit (claude-opus-5, claude-fable-5,
// claude-3-5-sonnet-20241022, claude-haiku-4-5-20251001, claude-2.1,
// claude-instant-1.2, claude-3-5-sonnet-latest, ...) while class ids are
// digit-free words (claude-frontier, claude-fast), so
// "no digit, no dot, no uppercase" separates the namespaces. The false-positive
// cost is severe (refusing a genuine Anthropic id), which is why the rule is an
// allowlist of the class shape rather than a denylist of known model shapes.
var classShapedRe = regexp.MustCompile(`^claude-[a-z]+(?:-[a-z]+)*(?:\[1m\])?$`)

// classShapedModel reports whether model sits in the workload-class namespace.
func classShapedModel(model string) bool {
	return classShapedRe.MatchString(model)
}

// classShapedRefusalReason is rule 3: every class-shaped id that reaches it is
// necessarily unknown — active rows matched matchRegistryClass and tombstones
// matched matchClassTombstone. Refuse typed; never
// let it fall through to Anthropic lane inference.
//
// The three ways to be unknown are not the same situation, and answering all of
// them with one sentence sent a real caller chasing a phantom "removed class"
// for a whole session. retrySecs > 0 marks the one case where trying again
// actually helps; the other two need the caller to change the request, so the
// message names the exact edit.
func classShapedRefusalReason(model string) (reason string, retrySecs int, ok bool) {
	if !classShapedModel(model) {
		return "", 0, false
	}

	// (1) Nothing has ever been loaded. Two different situations wear this
	// shape and only one is worth retrying, so split them on whether the brain
	// has ever answered: no answer yet means we are still coming up (or the
	// brain is down) and the id may well resolve on its own; an answer that
	// carried no classes means this deployment serves none, and no amount of
	// retrying will change that.
	if !classRowsLoaded() {
		if brainAnswered() {
			return "this proxy currently serves no workload classes at all — the brain's /preferred " +
				"was read successfully and declared none. Retrying will not help", 0, true
		}
		return "the workload-class registry has not loaded yet — this proxy has not had a successful read of " +
			"the brain's /preferred (it just started, or the brain is unreachable), so EVERY class id is " +
			"unknown right now, not just this one. Retry shortly; if it persists, check brain_reachable at " +
			statusHint(), 5, true
	}

	// (2) The class exists, but the caller asked for a 1M variant of it that
	// this proxy does not advertise. "[1m]" is renderer-owned metadata that
	// appears iff the row's window clears the 1M floor (registry.go), so a
	// class declared at 200k has no [1m] form to route to. Serving the bare
	// class anyway would hand back a 200k context to a caller that believes it
	// holds 1M, so the refusal stands — but it names the edit.
	if base, cut := strings.CutSuffix(model, "[1m]"); cut {
		if e, exists := matchRegistryClass(base); exists {
			return "class " + base + " exists but is not offered with a 1M context window, so \"" + base +
				"[1m]\" is not a model this proxy advertises (\"[1m]\" is picker metadata the proxy appends " +
				"only when a row's window clears the 1M floor; this class declares " +
				strconv.Itoa(e.ContextWindow) + "). Retrying will not help — send \"" + base +
				"\" without the suffix. Do NOT raise the class's context_window to force the marker: that number is " +
				"the MIN across the class's members, so inflating it lets a session outgrow its smallest member's " +
				"real window and earn a hard upstream refusal", 0, true
		}
	}

	// (3) Genuinely unknown: stale, misspelled, or removed while state was lost.
	return "class-shaped model id is not registered and enabled (workload-class ids are digit-free; " +
		"a class this proxy no longer knows is refused rather than forwarded upstream). Retrying will not help — " +
		"the classes this proxy currently serves are listed at " + statusHint(), 0, true
}

// brainAnswered reports whether the brain is reachable now (pref.ok: the
// /events stream is live and its last document decoded), not whether it ever
// answered. It is the only thing that separates "we cannot hear the brain"
// from "we asked and there are none" — and telling a caller to retry the second one is the same species
// of lie this refusal exists to stop telling.
func brainAnswered() bool {
	pref.mu.RLock()
	defer pref.mu.RUnlock()
	return pref.ok
}

// classRowsLoaded reports whether ANY source has populated the class rows yet
// (the boot file or a live /preferred). False only in the window between
// listening and the first load.
func classRowsLoaded() bool {
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	return classRowsSource != ""
}

// classRowGuard vets a candidate class row before it may enter the store,
// returning a non-"" human reason when it must be skipped. Shared by both
// entry paths (live derive and the last-good loader) so the store invariant
// holds regardless of where a row came from. The load-bearing check is the
// shape one: everything the store might advertise must satisfy
// classShapedModel, or rule 3 could never refuse the id after a state loss —
// see the module comment ("never advertise what can't be refused").
func classRowGuard(e *registryEntry) string {
	if strings.Contains(strings.ToLower(e.Name), "[1m]") {
		return "\"[1m]\" is renderer-owned, never part of an id"
	}
	id := e.renderedID()
	if !classShapedModel(id) {
		return "rendered id " + id + " is outside the digit-free class shape, so the rule-3 refusal " +
			"could never catch it after a state loss — never advertise what can't be refused " +
			"(rename the class to lowercase hyphenated words)"
	}
	return ""
}

// deriveClassRows builds the class registry rows from a /preferred classes
// payload, carrying forward a tombstone for every previously-known class id
// (active or already tombstoned) the fresh payload no longer contains. A
// re-created class simply derives active again — the tombstone is dropped.
// claudeCodeShape is the request_shape a consumer must declare for its classes
// to appear in the Claude Code picker (#95). Anything else is another app's
// class: other apps own theirs, never select from a picker, and
// have no use for a window hint. A class with NO consumer stays advertised —
// unowned is not the same as owned-by-something-else.
const claudeCodeShape = "claude-code"

func pickerHiddenFor(consumer string, shapes map[string]string) bool {
	if consumer == "" {
		return false
	}
	shape, known := shapes[consumer]
	return known && shape != claudeCodeShape
}

var classMemberDisplayRe = regexp.MustCompile(`^claude-([a-z]+)-(\d+)(?:-(\d{1,2}))?(?:-\d{8})?$`)

// classMemberDisplay names the designed first choice without provider branding
// guesses: only recognized Anthropic model ids receive a human-readable name.
func classMemberDisplay(m classMemberPref) string {
	if m.Kind == kindAnthropic {
		if parts := classMemberDisplayRe.FindStringSubmatch(m.Model); parts != nil {
			name := "Claude " + strings.ToUpper(parts[1][:1]) + parts[1][1:] + " " + parts[2]
			if parts[3] != "" {
				name += "." + parts[3]
			}
			return name
		}
	}
	return m.Model
}

// classDirectModel is the model a session that bypasses the proxy runs the class
// on (`claude-switch run --direct`): the first Anthropic member in priority
// order, since a provider member is only reachable through the proxy. Empty
// when there is none, and the client then has no direct stand-in for the class.
func classDirectModel(sorted []classMemberPref) string {
	for _, m := range sorted {
		if m.Kind == kindAnthropic && m.Model != "" {
			return m.Model
		}
	}
	return ""
}

func deriveClassRows(classes map[string]classPref, prev []registryEntry, shapes map[string]string) []registryEntry {
	out := make([]registryEntry, 0, len(classes))
	for name, c := range classes {
		if name == "" {
			log.Printf("class rows: ignoring class with empty id from /preferred")
			continue
		}
		e := registryEntry{Name: name, Type: "class", Class: name,
			Display: c.Display, ContextWindow: c.ContextWindow, Guidance: c.Guidance,
			pickerHidden: pickerHiddenFor(c.Consumer, shapes)}
		if c.RequestKind != "" && c.RequestKind != "chat" {
			e.Modality = c.RequestKind
			// An image class is reached through its own door, never a picker:
			// a chat client that chose it would be refused.
			e.pickerHidden = true
		}
		// Guidance describes the designed first choice, never the live pointer.
		// Sort a copy: other readers retain the brain's original member payload.
		members := slices.Clone(c.Members)
		sort.SliceStable(members, func(i, j int) bool { return members[i].Priority < members[j].Priority })
		if len(members) > 0 {
			e.FirstChoice = classMemberDisplay(members[0])
		}
		e.DirectModel = classDirectModel(members)
		if e.Display == "" {
			e.Display = name
		}
		// The window is advertised as the brain declared it, smaller than
		// 200k included (the local class's 65536, #135). Claude Code still
		// infers 200k from a bare claude-* id — a client limitation the
		// README states — but every reader of context_window gets the truth.
		if why := classRowGuard(&e); why != "" {
			log.Printf("class rows: ignoring class %q — %s", name, why)
			continue
		}
		out = append(out, e)
	}
	for _, p := range prev {
		if p.Class == "" {
			continue
		}
		if _, has := classes[p.Class]; has {
			continue
		}
		t := p
		t.disabledReason = classRemovedReason
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// updateClassRowsFromPreferred is fetchPreferred's hook: derive against the
// current snapshot (so vanished ids tombstone) and install the result. Called
// only when the /preferred payload CARRIED a classes key — an old brain that
// omits the key entirely says nothing about classes, and tombstoning the world
// on its silence would turn a brain rollback into an advertising outage.
func updateClassRowsFromPreferred(classes map[string]classPref, shapes map[string]string) {
	classRowsMu.RLock()
	prev := classRowsSet
	classRowsMu.RUnlock()
	setClassRows(deriveClassRows(classes, prev, shapes), "preferred")
}

// setClassRows installs rows (an atomic reference swap under the lock) and, on
// a live-derived CHANGE, persists the new last-good snapshot. Boot-time loads
// pass source "last-good" and never re-persist what they just read.
func setClassRows(rows []registryEntry, source string) {
	classRowsMu.Lock()
	changed := !slices.Equal(classRowsSet, rows)
	classRowsSet, classRowsSource = rows, source
	classRowsMu.Unlock()
	if !changed || source != "preferred" {
		return
	}
	active, dead := 0, 0
	ids := make([]string, 0, len(rows))
	for i := range rows {
		if rows[i].disabledReason == "" {
			active++
			ids = append(ids, rows[i].renderedID())
		} else {
			dead++
		}
	}
	log.Printf("class rows: derived from /preferred — %d active (%s), %d tombstoned", active, strings.Join(ids, ", "), dead)
	persistClassRows(rows)
}

// classRowsSnapshot returns the current rows and their source for status views.
func classRowsSnapshot() ([]registryEntry, string) {
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	return classRowsSet, classRowsSource
}

// registryClassRows returns the advertised rows for ACTIVE class entries. A
// class row has no fixed upstream — the brain resolves it per-session (#27).
func registryClassRows() []advertisedModel {
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	out := []advertisedModel{}
	for i := range classRowsSet {
		e := &classRowsSet[i]
		if e.disabledReason == "" && !e.pickerHidden {
			out = append(out, advertisedModel{pickerID: e.renderedID(), display: e.Display,
				kind: kindClass, effort: classEffortReaches(e.Class),
				contextWindow: e.ContextWindow})
		}
	}
	return out
}

// classTargetMember is the member a class currently points at: the brain's
// pointer when it names one of the members, else the highest-priority member
// (/preferred serves Members priority-sorted). false = no answer — the class is
// not in the last /preferred, or that payload gave it no members.
//
// LOCK ORDER: pref.mu is taken WITHOUT classRowsMu, and never the other way
// round. registryClassRows holds classRowsMu while it calls
// classEffortReaches, so this must stay a leaf on pref.mu only; fetchPreferred
// releases pref.mu before it touches the class rows at all.
func classTargetMember(class string) (classMemberPref, bool) {
	if class == "" {
		return classMemberPref{}, false
	}
	pref.mu.RLock()
	cp, ok := pref.classes[class]
	pref.mu.RUnlock()
	if !ok || len(cp.Members) == 0 {
		return classMemberPref{}, false
	}
	if cp.Pointer != nil {
		for _, m := range cp.Members {
			if m.ID == *cp.Pointer {
				return m, true
			}
		}
	}
	return cp.Members[0], true
}

// classEffortReaches is the §15.2 `effort` bool for a class row: whether
// Claude Code's effort setting reaches the model the class targets RIGHT NOW.
// An anthropic member is a lane hop with the body untouched (true); a gpt
// provider member translates effort through codexWire (true). Kimi forwards it
// to an upstream with no effort parameter (false).
//
// Unknown targets answer false, deliberately: brain-down the class rows still
// serve from the last-good snapshot (that is the point of the snapshot) but the
// MEMBER view is not persisted, so we cannot say the setting lands. A consumer
// hiding the effort control is a smaller lie than offering a control that
// silently does nothing.
func classEffortReaches(class string) bool {
	m, ok := classTargetMember(class)
	return ok && (m.Kind == kindAnthropic || (m.Kind == "provider" && providerEffortReaches(m.Provider)))
}

// matchRegistryClass reports whether model is an ACTIVE class row's rendered
// id — the entry to the class-routing branch in requestHandler. Checked after
// matchProvider and before matchClassTombstone. Returns a copy: the poll
// loop swaps the underlying slice while requests are in flight.
// classIDMatches reports whether model addresses this class row. The rendered id
// is canonical; a MARKED row additionally answers to its bare form. The "[1m]"
// suffix is Claude Code window metadata, and the API consumers of a class never
// see a picker: an API client sends "tutor" or "claude-tutor". Once the
// effective-window floor (#120) started marking such rows, matching on the
// rendered id ALONE would have answered
// those clients model_not_found — an outage, not a degradation.
//
// The reverse stays refused: an unmarked row never answers to a suffixed id, so
// a stale picker offering "claude-frontier[1m]" is still the typed rule-3
// refusal. Only the missing-suffix direction is an alias, and it resolves to the
// class row, never to lane inference.
func classIDMatches(e *registryEntry, model string) bool {
	if model == e.renderedID() {
		return true
	}
	return strings.HasSuffix(e.renderedID(), "[1m]") && model == "claude-"+e.Name
}

func matchRegistryClass(model string) (*registryEntry, bool) {
	if model == "" {
		return nil, false
	}
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	for i := range classRowsSet {
		if classRowsSet[i].disabledReason == "" && classIDMatches(&classRowsSet[i], model) {
			e := classRowsSet[i]
			return &e, true
		}
	}
	return nil, false
}

// matchRegistryClassName is the live class row of brain class name, for a
// door that routes into one class by name rather than by a picker id.
func matchRegistryClassName(name string) (*registryEntry, bool) {
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	for i := range classRowsSet {
		if classRowsSet[i].disabledReason == "" && classRowsSet[i].Class == name {
			e := classRowsSet[i]
			return &e, true
		}
	}
	return nil, false
}

// matchClassTombstone reports whether model is a TOMBSTONED class row's
// rendered id (rule 2): recognized, answered with the existing typed 503.
func matchClassTombstone(model string) (*registryEntry, bool) {
	if model == "" {
		return nil, false
	}
	classRowsMu.RLock()
	defer classRowsMu.RUnlock()
	for i := range classRowsSet {
		if classRowsSet[i].disabledReason != "" && classIDMatches(&classRowsSet[i], model) {
			e := classRowsSet[i]
			return &e, true
		}
	}
	return nil, false
}

// ---- last-good persistence ----

// classStateRow is one persisted row. Deliberately its own shape (not a raw
// registryEntry): disabledReason is unexported, and the file should only ever
// hold the class fields — a hand-edited entry of another type must not round-trip.
type classStateRow struct {
	Name          string `json:"name"`
	Display       string `json:"display"`
	Class         string `json:"class"`
	ContextWindow int    `json:"context_window"`
	Guidance      string `json:"guidance,omitempty"`
	FirstChoice   string `json:"first_choice,omitempty"`
	DirectModel   string `json:"direct_model,omitempty"`
	Removed       bool   `json:"removed,omitempty"`
	PickerHidden  bool   `json:"pickerHidden,omitempty"`
	// Modality is the row's (registry.go): "" for chat, "image" for an image
	// class, so a boot with no brain still knows the image door's classes.
	Modality string `json:"modality,omitempty"`
}

type classStateFile struct {
	SavedAt int64           `json:"savedAt"`
	Rows    []classStateRow `json:"rows"`
}

// persistClassRows writes the last-good snapshot with the same atomic
// tmp+rename pattern as writeClientModelsCache. A write failure is logged and
// tolerated: persistence is durability, not correctness — the in-memory rows
// keep serving.
func persistClassRows(rows []registryEntry) {
	if classStateFilePath == "" {
		return
	}
	f := classStateFile{SavedAt: time.Now().UnixMilli()}
	for i := range rows {
		e := &rows[i]
		f.Rows = append(f.Rows, classStateRow{Name: e.Name, Display: e.Display, Class: e.Class,
			ContextWindow: e.ContextWindow, Removed: e.disabledReason != "",
			PickerHidden: e.pickerHidden, Guidance: e.Guidance, FirstChoice: e.FirstChoice,
			DirectModel: e.DirectModel, Modality: e.Modality})
	}
	buf, err := json.Marshal(f)
	if err != nil {
		log.Printf("class rows: cannot marshal state: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(classStateFilePath), 0o700); err != nil {
		log.Printf("class rows: cannot create state dir: %v", err)
		return
	}
	tmp := classStateFilePath + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		log.Printf("class rows: cannot write state: %v", err)
		return
	}
	if err := os.Rename(tmp, classStateFilePath); err != nil {
		log.Printf("class rows: cannot install state: %v", err)
	}
}

// loadClassRowsState seeds the store from the last-good snapshot at boot, so
// advertising works brain-down from the first request. Any failure is logged
// and non-fatal (a supervisor's restart would crash-loop a fatal here); the store
// simply starts empty and fills on the first live /preferred.
//
// Called in main() AFTER buildHandler (the registry must exist: classRowGuard's
// named-collision check keeps a stale snapshot row from double-advertising a
// picker id a named models.json entry now owns) and BEFORE the poll goroutine
// and the listener start (a live /preferred must replace the snapshot, never
// the reverse — and the rows must be in place for the first request).
func loadClassRowsState() {
	if classStateFilePath == "" {
		return
	}
	raw, err := os.ReadFile(classStateFilePath)
	if err != nil {
		if !os.IsNotExist(err) { // absent file is a normal first boot, not an error
			log.Printf("class rows: cannot read last-good state %s: %v — starting empty", classStateFilePath, err)
		}
		return
	}
	var f classStateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		log.Printf("class rows: bad JSON in %s: %v — starting empty", classStateFilePath, err)
		return
	}
	rows := make([]registryEntry, 0, len(f.Rows))
	active, dead := 0, 0
	for _, r := range f.Rows {
		if r.Name == "" || r.Class == "" {
			continue
		}
		e := registryEntry{Name: r.Name, Type: "class", Class: r.Class, pickerHidden: r.PickerHidden,
			Display: r.Display, ContextWindow: r.ContextWindow, Guidance: r.Guidance, FirstChoice: r.FirstChoice,
			DirectModel: r.DirectModel, Modality: r.Modality}
		if e.Display == "" {
			e.Display = e.Name
		}
		if why := classRowGuard(&e); why != "" {
			// The same admission rule as the live derive: a snapshot is
			// last-good state, not an authority — a hand-edited or stale row
			// must not smuggle in what the derive path would refuse.
			log.Printf("class rows: dropping last-good row %q — %s", r.Name, why)
			continue
		}
		if r.Removed {
			e.disabledReason = classRemovedReason
			dead++
		} else {
			active++
		}
		rows = append(rows, e)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	setClassRows(rows, "last-good")
	log.Printf("class rows: boot from last-good %s — %d active, %d tombstoned (a live /preferred will replace them)",
		classStateFilePath, active, dead)
}
