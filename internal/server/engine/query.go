package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/larsla/takt/api"
)

// The visibility query language is a subset of Cadence's advanced
// visibility queries:
//
//	WorkflowType = 'order' AND (CloseStatus = 'FAILED' OR CloseTime = missing)
//	StartTime > '2026-01-01T00:00:00Z' ORDER BY StartTime ASC
//
// Fields are WorkflowID, RunID, WorkflowType, TaskList, CloseStatus,
// StartTime, CloseTime, ExecutionTime and custom search attributes.

type predicate func(*VisibilityRecord) bool

type visQuery struct {
	match     predicate
	orderBy   string
	ascending bool
}

type token struct {
	kind string // ident, string, number, op, lparen, rparen, eof
	text string
}

func lex(q string) ([]token, error) {
	var out []token
	rs := []rune(q)
	for i := 0; i < len(rs); {
		r := rs[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '(':
			out = append(out, token{"lparen", "("})
			i++
		case r == ')':
			out = append(out, token{"rparen", ")"})
			i++
		case r == '\'' || r == '"':
			j := i + 1
			for j < len(rs) && rs[j] != r {
				j++
			}
			if j == len(rs) {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			out = append(out, token{"string", string(rs[i+1 : j])})
			i = j + 1
		case strings.ContainsRune("=!<>", r):
			j := i + 1
			if j < len(rs) && rs[j] == '=' {
				j++
			}
			op := string(rs[i:j])
			if op == "!" {
				return nil, fmt.Errorf("unexpected '!' at %d", i)
			}
			out = append(out, token{"op", op})
			i = j
		case unicode.IsDigit(r) || r == '-':
			j := i + 1
			for j < len(rs) && (unicode.IsDigit(rs[j]) || rs[j] == '.') {
				j++
			}
			out = append(out, token{"number", string(rs[i:j])})
			i = j
		case unicode.IsLetter(r) || r == '_':
			j := i + 1
			for j < len(rs) && (unicode.IsLetter(rs[j]) || unicode.IsDigit(rs[j]) || rs[j] == '_' || rs[j] == '.') {
				j++
			}
			out = append(out, token{"ident", string(rs[i:j])})
			i = j
		default:
			return nil, fmt.Errorf("unexpected %q at %d", r, i)
		}
	}
	return append(out, token{"eof", ""}), nil
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != "eof" {
		p.pos++
	}
	return t
}

func (p *parser) keyword(word string) bool {
	t := p.peek()
	if t.kind == "ident" && strings.EqualFold(t.text, word) {
		p.pos++
		return true
	}
	return false
}

// parseQuery compiles a visibility query. An empty query matches everything.
func parseQuery(q string) (*visQuery, error) {
	toks, err := lex(q)
	if err != nil {
		return nil, badRequest("invalid query: %v", err)
	}
	p := &parser{toks: toks}
	vq := &visQuery{match: func(*VisibilityRecord) bool { return true }, orderBy: "StartTime"}
	startsWithOrder := p.peek().kind == "ident" && strings.EqualFold(p.peek().text, "ORDER")
	if p.peek().kind != "eof" && !startsWithOrder {
		if vq.match, err = p.or(); err != nil {
			return nil, badRequest("invalid query: %v", err)
		}
	}
	if p.keyword("ORDER") {
		if !p.keyword("BY") {
			return nil, badRequest("invalid query: expected BY after ORDER")
		}
		t := p.next()
		if t.kind != "ident" {
			return nil, badRequest("invalid query: expected a field after ORDER BY")
		}
		vq.orderBy = t.text
		switch {
		case p.keyword("ASC"):
			vq.ascending = true
		case p.keyword("DESC"):
		}
	}
	if t := p.peek(); t.kind != "eof" {
		return nil, badRequest("invalid query: unexpected %q", t.text)
	}
	return vq, nil
}

func (p *parser) or() (predicate, error) {
	left, err := p.and()
	if err != nil {
		return nil, err
	}
	for p.keyword("OR") {
		right, err := p.and()
		if err != nil {
			return nil, err
		}
		l := left
		left = func(r *VisibilityRecord) bool { return l(r) || right(r) }
	}
	return left, nil
}

func (p *parser) and() (predicate, error) {
	left, err := p.factor()
	if err != nil {
		return nil, err
	}
	for p.keyword("AND") {
		right, err := p.factor()
		if err != nil {
			return nil, err
		}
		l := left
		left = func(r *VisibilityRecord) bool { return l(r) && right(r) }
	}
	return left, nil
}

func (p *parser) factor() (predicate, error) {
	if p.peek().kind == "lparen" {
		p.next()
		inner, err := p.or()
		if err != nil {
			return nil, err
		}
		if p.next().kind != "rparen" {
			return nil, fmt.Errorf("missing ')'")
		}
		return inner, nil
	}
	field := p.next()
	if field.kind != "ident" {
		return nil, fmt.Errorf("expected a field name, got %q", field.text)
	}
	op := p.next()
	if op.kind != "op" {
		return nil, fmt.Errorf("expected an operator after %s", field.text)
	}
	val := p.next()
	switch val.kind {
	case "string", "number":
	case "ident":
		if !strings.EqualFold(val.text, "missing") {
			return nil, fmt.Errorf("unquoted value %q", val.text)
		}
		val.kind = "missing"
	default:
		return nil, fmt.Errorf("expected a value after %s %s", field.text, op.text)
	}
	return comparison(field.text, op.text, val)
}

func comparison(field, op string, val token) (predicate, error) {
	switch strings.ToLower(field) {
	case "workflowid":
		return stringCmp(op, val, func(r *VisibilityRecord) string { return r.WorkflowID })
	case "runid":
		return stringCmp(op, val, func(r *VisibilityRecord) string { return r.RunID })
	case "workflowtype":
		return stringCmp(op, val, func(r *VisibilityRecord) string { return r.WorkflowType })
	case "tasklist":
		return stringCmp(op, val, func(r *VisibilityRecord) string { return r.TaskList })
	case "closestatus":
		want, err := closeStatusValue(val)
		if err != nil {
			return nil, err
		}
		v := token{kind: "string", text: string(want)}
		return stringCmp(op, v, func(r *VisibilityRecord) string { return string(r.CloseStatus) })
	case "starttime":
		return timeCmp(op, val, func(r *VisibilityRecord) int64 { return r.StartTime })
	case "closetime":
		return timeCmp(op, val, func(r *VisibilityRecord) int64 { return r.CloseTime })
	case "executiontime":
		return timeCmp(op, val, func(r *VisibilityRecord) int64 { return r.ExecutionTime })
	default:
		return attrCmp(field, op, val)
	}
}

// closeStatusValue accepts a status name or Cadence's numeric value.
func closeStatusValue(val token) (api.WorkflowExecutionCloseStatus, error) {
	statuses := []api.WorkflowExecutionCloseStatus{
		api.CloseStatusCompleted, api.CloseStatusFailed, api.CloseStatusCanceled,
		api.CloseStatusTerminated, api.CloseStatusContinuedAsNew, api.CloseStatusTimedOut,
	}
	if val.kind == "number" {
		n, err := strconv.Atoi(val.text)
		if err != nil || n < 0 || n >= len(statuses) {
			return "", fmt.Errorf("unknown close status %s", val.text)
		}
		return statuses[n], nil
	}
	for _, s := range statuses {
		if strings.EqualFold(string(s), val.text) || strings.EqualFold(strings.ReplaceAll(string(s), "_", ""), val.text) {
			return s, nil
		}
	}
	return "", fmt.Errorf("unknown close status %q", val.text)
}

func stringCmp(op string, val token, get func(*VisibilityRecord) string) (predicate, error) {
	if val.kind == "missing" {
		return missingCmp(op, func(r *VisibilityRecord) bool { return get(r) == "" })
	}
	want := val.text
	switch op {
	case "=":
		return func(r *VisibilityRecord) bool { return get(r) == want }, nil
	case "!=":
		return func(r *VisibilityRecord) bool { return get(r) != want }, nil
	case "<":
		return func(r *VisibilityRecord) bool { return get(r) < want }, nil
	case "<=":
		return func(r *VisibilityRecord) bool { return get(r) <= want }, nil
	case ">":
		return func(r *VisibilityRecord) bool { return get(r) > want }, nil
	case ">=":
		return func(r *VisibilityRecord) bool { return get(r) >= want }, nil
	}
	return nil, fmt.Errorf("unknown operator %s", op)
}

func missingCmp(op string, missing func(*VisibilityRecord) bool) (predicate, error) {
	switch op {
	case "=":
		return missing, nil
	case "!=":
		return func(r *VisibilityRecord) bool { return !missing(r) }, nil
	}
	return nil, fmt.Errorf("only = and != work with missing")
}

func parseTimeValue(val token) (int64, error) {
	if val.kind == "number" {
		return strconv.ParseInt(val.text, 10, 64)
	}
	t, err := time.Parse(time.RFC3339Nano, val.text)
	if err != nil {
		return 0, fmt.Errorf("invalid time %q: use RFC 3339 or Unix nanoseconds", val.text)
	}
	return t.UnixNano(), nil
}

func numCmp(op string, want float64, get func(*VisibilityRecord) (float64, bool)) (predicate, error) {
	cmp := map[string]func(a, b float64) bool{
		"=":  func(a, b float64) bool { return a == b },
		"!=": func(a, b float64) bool { return a != b },
		"<":  func(a, b float64) bool { return a < b },
		"<=": func(a, b float64) bool { return a <= b },
		">":  func(a, b float64) bool { return a > b },
		">=": func(a, b float64) bool { return a >= b },
	}[op]
	if cmp == nil {
		return nil, fmt.Errorf("unknown operator %s", op)
	}
	return func(r *VisibilityRecord) bool {
		v, ok := get(r)
		return ok && cmp(v, want)
	}, nil
}

func timeCmp(op string, val token, get func(*VisibilityRecord) int64) (predicate, error) {
	if val.kind == "missing" {
		return missingCmp(op, func(r *VisibilityRecord) bool { return get(r) == 0 })
	}
	want, err := parseTimeValue(val)
	if err != nil {
		return nil, err
	}
	return numCmp(op, float64(want), func(r *VisibilityRecord) (float64, bool) {
		v := get(r)
		return float64(v), v != 0
	})
}

// attrCmp compares a custom search attribute, stored as JSON.
func attrCmp(name, op string, val token) (predicate, error) {
	get := func(r *VisibilityRecord) (interface{}, bool) {
		if r.SearchAttributes == nil {
			return nil, false
		}
		raw, ok := r.SearchAttributes.IndexedFields[name]
		if !ok {
			return nil, false
		}
		var v interface{}
		if json.Unmarshal(raw, &v) != nil {
			return nil, false
		}
		return v, true
	}
	if val.kind == "missing" {
		return missingCmp(op, func(r *VisibilityRecord) bool { _, ok := get(r); return !ok })
	}
	if val.kind == "number" {
		want, err := strconv.ParseFloat(val.text, 64)
		if err != nil {
			return nil, err
		}
		return numCmp(op, want, func(r *VisibilityRecord) (float64, bool) {
			v, ok := get(r)
			f, isNum := v.(float64)
			return f, ok && isNum
		})
	}
	return stringCmp(op, val, func(r *VisibilityRecord) string {
		v, ok := get(r)
		if !ok {
			return ""
		}
		if s, isStr := v.(string); isStr {
			return s
		}
		return fmt.Sprint(v)
	})
}

// sortRecords orders records for a query.
func (q *visQuery) sortRecords(recs []*VisibilityRecord) {
	key := func(r *VisibilityRecord) int64 {
		switch strings.ToLower(q.orderBy) {
		case "closetime":
			return r.CloseTime
		case "executiontime":
			return r.ExecutionTime
		default:
			return r.StartTime
		}
	}
	sort.SliceStable(recs, func(i, j int) bool {
		a, b := key(recs[i]), key(recs[j])
		if a == b {
			return recs[i].RunID < recs[j].RunID
		}
		if q.ascending {
			return a < b
		}
		return a > b
	})
}
