package engine

import (
	"testing"
	"time"

	"github.com/larsla/takt/api"
)

func TestParseQuery(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	recs := []*VisibilityRecord{
		{WorkflowID: "a1", RunID: "r1", WorkflowType: "order", TaskList: "tl", StartTime: t0,
			SearchAttributes: &api.SearchAttributes{IndexedFields: map[string][]byte{"Customer": []byte(`"acme"`), "Amount": []byte(`42`)}}},
		{WorkflowID: "a2", RunID: "r2", WorkflowType: "order", StartTime: t0 + 10, CloseTime: t0 + 20, CloseStatus: api.CloseStatusFailed},
		{WorkflowID: "b1", RunID: "r3", WorkflowType: "billing", StartTime: t0 + 30, CloseTime: t0 + 40, CloseStatus: api.CloseStatusTerminated},
	}
	cases := map[string][]string{
		``:                           {"r3", "r2", "r1"},
		`WorkflowType = 'order'`:     {"r2", "r1"},
		`WorkflowType != "order"`:    {"r3"},
		`CloseTime = missing`:        {"r1"},
		`CloseTime != missing`:       {"r3", "r2"},
		`CloseStatus = 'FAILED'`:     {"r2"},
		`CloseStatus = 3`:            {"r3"},
		`closestatus = 'terminated'`: {"r3"},
		`WorkflowType = 'order' AND CloseTime = missing`:               {"r1"},
		`WorkflowType = 'billing' OR CloseStatus = 'FAILED'`:           {"r3", "r2"},
		`(WorkflowID = 'a1' OR WorkflowID = 'b1') AND TaskList = 'tl'`: {"r1"},
		`StartTime >= '2026-01-01T00:00:00Z' ORDER BY StartTime ASC`:   {"r1", "r2", "r3"},
		`StartTime > 0 ORDER BY CloseTime DESC`:                        {"r3", "r2", "r1"},
		`CloseTime < 4000000000000000000 ORDER BY ExecutionTime`:       {"r2", "r3"},
		`Customer = 'acme'`:                      {"r1"},
		`Amount > 40 AND Amount <= 42`:           {"r1"},
		`Amount = missing`:                       {"r3", "r2"},
		`RunID <= 'r2' AND RunID >= 'r2'`:        {"r2"},
		`WorkflowID < 'b' AND WorkflowID > 'a1'`: {"r2"},
	}
	for q, want := range cases {
		vq, err := parseQuery(q)
		if err != nil {
			t.Errorf("%q: %v", q, err)
			continue
		}
		var got []*VisibilityRecord
		for _, r := range recs {
			if vq.match(r) {
				got = append(got, r)
			}
		}
		vq.sortRecords(got)
		var ids []string
		for _, r := range got {
			ids = append(ids, r.RunID)
		}
		if len(ids) != len(want) {
			t.Errorf("%q: got %v, want %v", q, ids, want)
			continue
		}
		for i := range ids {
			if ids[i] != want[i] {
				t.Errorf("%q: got %v, want %v", q, ids, want)
				break
			}
		}
	}

	for _, bad := range []string{
		`WorkflowType = order`, `WorkflowType 'x'`, `= 'x'`, `WorkflowType = 'x' AND`, `(WorkflowType = 'x'`,
		`WorkflowType = 'unterminated`, `CloseStatus = 'BOGUS'`, `CloseStatus = 9`, `StartTime > 'yesterday'`,
		`CloseTime > missing`, `WorkflowType ! 'x'`, `ORDER StartTime`, `ORDER BY`, `WorkflowType = 'x' extra`, `#`,
	} {
		if _, err := parseQuery(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
