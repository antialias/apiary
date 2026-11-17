package main

// The window-record golden fixtures (quota-model 64 §5): the brain holds a
// reported refusal by this proxy's rule, and one file of cases checks both
// sides. Each case's steps run through pruneRecords, the path that ends a
// record or learns its end after every brain read, and each window case
// through recordWindow; the brain's test_exporter.py (RecordsGoldenTest)
// runs the same file through its _hold_judge and _hold_window.
// Regenerate the file, never hand-edit one side.

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// goldenRecord is a refusal as POST /reports carries it.
type goldenRecord struct {
	Window    string   `json:"window"`
	ReadAt    float64  `json:"read_at"`
	Until     *float64 `json:"until"`
	ResetsAt  *float64 `json:"resets_at"`
	ByReading bool     `json:"by_reading"`
	Seen      struct {
		UsedPct  *float64 `json:"used_pct"`
		ResetsAt *float64 `json:"resets_at"`
	} `json:"seen"`
}

// refusalRecord is the record the proxy keeps for it (writeRecord's fields).
func (g goldenRecord) refusalRecord() refusalRecord {
	r := refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: goldenTime(g.ReadAt),
		attributed: g.ByReading,
		seen:       limitSeen{usedPct: g.Seen.UsedPct, resetsAt: g.Seen.ResetsAt}}
	if g.Until != nil {
		r.until = goldenTime(*g.Until)
	}
	if g.ResetsAt != nil {
		r.resets = goldenTime(*g.ResetsAt)
	}
	return r
}

type recordsGoldenDoc struct {
	ResetSlackS float64 `json:"reset_slack_s"`
	Cases       []struct {
		Name   string       `json:"name"`
		Record goldenRecord `json:"record"`
		Steps  []struct {
			Standing acctStanding `json:"standing"`
			Now      float64      `json:"now"`
			Want     struct {
				Ends  bool     `json:"ends"`
				Until *float64 `json:"until"`
			} `json:"want"`
		} `json:"steps"`
	} `json:"cases"`
	Windows []struct {
		Name   string       `json:"name"`
		Record goldenRecord `json:"record"`
		Window checkWindow  `json:"window"`
		Now    float64      `json:"now"`
		Want   struct {
			UsedPct    *float64 `json:"used_pct"`
			ResetsAt   *float64 `json:"resets_at"`
			AsOf       *float64 `json:"as_of"`
			ResetBasis *string  `json:"reset_basis"`
		} `json:"want"`
	} `json:"windows"`
}

func goldenTime(s float64) time.Time { return time.Unix(0, int64(s*1e9)) }

func loadRecordsGolden(t *testing.T) recordsGoldenDoc {
	t.Helper()
	b, err := os.ReadFile("testdata/records_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var d recordsGoldenDoc
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestRecordWindowGolden(t *testing.T) {
	d := loadRecordsGolden(t)
	if len(d.Windows) < 8 {
		t.Fatalf("only %d golden window cases loaded", len(d.Windows))
	}
	for _, c := range d.Windows {
		t.Run(c.Name, func(t *testing.T) {
			got := recordWindow(c.Record.refusalRecord(), c.Window, goldenTime(c.Now))
			var basis *string
			if got.ResetBasis != "" {
				basis = &got.ResetBasis
			}
			for _, f := range []struct {
				name      string
				got, want any
			}{
				{"used_pct", fmtUntil(got.UsedPct), fmtUntil(c.Want.UsedPct)},
				{"resets_at", fmtUntil(got.ResetsAt), fmtUntil(c.Want.ResetsAt)},
				{"as_of", fmtUntil(got.AsOf), fmtUntil(c.Want.AsOf)},
				{"reset_basis", fmtStr(basis), fmtStr(c.Want.ResetBasis)},
			} {
				if f.got != f.want {
					t.Errorf("%s = %v, want %v", f.name, f.got, f.want)
				}
			}
		})
	}
}

func fmtStr(s *string) any {
	if s == nil {
		return "null"
	}
	return *s
}

func TestRecordsGolden(t *testing.T) {
	d := loadRecordsGolden(t)
	if len(d.Cases) < 20 {
		t.Fatalf("only %d golden cases loaded", len(d.Cases))
	}
	if d.ResetSlackS != recResetSlack.Seconds() {
		t.Fatalf("golden reset slack %vs, recResetSlack %v", d.ResetSlackS, recResetSlack)
	}
	oldNow := nowFn
	lan.recMu.Lock()
	oldRecords := lan.records
	lan.records = map[recKey]refusalRecord{}
	lan.recMu.Unlock()
	t.Cleanup(func() {
		nowFn = oldNow
		lan.recMu.Lock()
		lan.records = oldRecords
		lan.recMu.Unlock()
	})
	for _, c := range d.Cases {
		t.Run(c.Name, func(t *testing.T) {
			key := recKey{"a", c.Record.Window}
			lan.recMu.Lock()
			lan.records = map[recKey]refusalRecord{key: c.Record.refusalRecord()}
			lan.recMu.Unlock()
			for i, s := range c.Steps {
				now := goldenTime(s.Now)
				nowFn = func() time.Time { return now }
				if s.Standing.Allowance.Windows == nil {
					s.Standing.Allowance.Windows = map[string]checkWindow{}
				}
				lan.pruneRecords(map[string]acctStanding{"a": s.Standing})

				lan.recMu.Lock()
				got, held := lan.records[key]
				lan.recMu.Unlock()
				if held == s.Want.Ends {
					t.Fatalf("step %d: ends = %v, want %v", i, !held, s.Want.Ends)
				}
				if !held {
					return
				}
				var until *float64
				if !got.until.IsZero() {
					u := unixSeconds(got.until)
					until = &u
				}
				if fmtUntil(until) != fmtUntil(s.Want.Until) {
					t.Fatalf("step %d: until = %v, want %v", i, fmtUntil(until), fmtUntil(s.Want.Until))
				}
			}
		})
	}
}

func fmtUntil(u *float64) any {
	if u == nil {
		return "null"
	}
	return *u
}
