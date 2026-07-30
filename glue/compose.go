//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package glue

// CEP composition (DESIGN-cep.md Phase 4): a DAG of `.multi` packs where one
// pack's labelResults feed the next. LabelResults are just rows, so a pack's
// labelResults are themselves a keyspace a downstream pack can `FROM` — primitive
// detections feeding correlation/incident packs (Prometheus recording→alerting
// rules; dbt models ref()-ing models).
//
// Mechanism (the MVP the design calls for — materialize + re-poll, not true
// cross-layer delta): nodes run in topological order on ONE session; each node's
// labelResults are materialized into a TEMP KEYSPACE (temp_keyspace.go, named by
// ComposeKeyspace), which a downstream node reads with the collision-free table
// reference `FROM node('<up>')` (compose_node.go — a function ref can't clash with a
// keyspace name). Each materialized row is {label, result, fingerprint} — result stays
// nested (navigate x.result.<field>), label enables per-detector GROUP BY, and the
// fingerprint is a lineage handle.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ComposeNode is one DAG node: a named pack plus the upstream node names it reads
// (via `FROM node('<dep>')`). Needs edges are declared, not inferred.
type ComposeNode struct {
	Name    string
	Needs   []string
	Entries []MultiQueryEntry
}

// ComposeNodeResult is one node's output after the DAG runs. Rejected is set when
// the node's pack failed to parse/plan/convert (so Count is a "never ran" zero, not
// a "ran, matched nothing" zero) — Reason carries the parser message.
type ComposeNodeResult struct {
	Node         string
	Count        int
	LabelResults []LabelResult
	Rejected     bool
	Reason       string
}

// ComposeResult is the whole DAG run: the topological order and each node's rows.
type ComposeResult struct {
	Order []string
	Nodes []ComposeNodeResult
}

// ComposeKeyspace is the INTERNAL temp-keyspace name a node's labelResults
// materialize under. The `pack_` prefix is not user-facing — a downstream node reads
// the rows via `FROM node('<name>')` (compose_node.go resolves the name here), never
// by this keyspace name. Kept prefixed so the internal keyspace is unlikely to shadow
// a real one.
func ComposeKeyspace(node string) string { return "pack_" + node }

// TopoOrder returns the node names in dependency order (every node after all its
// Needs), erroring on an unknown dependency or a cycle. Deterministic: siblings
// resolve in name order.
func TopoOrder(nodes []ComposeNode) ([]string, error) {
	byName := make(map[string]*ComposeNode, len(nodes))
	names := make([]string, 0, len(nodes))
	for i := range nodes {
		if _, dup := byName[nodes[i].Name]; dup {
			return nil, fmt.Errorf("duplicate node %q", nodes[i].Name)
		}
		byName[nodes[i].Name] = &nodes[i]
		names = append(names, nodes[i].Name)
	}
	sort.Strings(names)

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(nodes))
	var order []string
	var visit func(n string, path []string) error
	visit = func(n string, path []string) error {
		switch color[n] {
		case black:
			return nil
		case gray:
			return fmt.Errorf("dependency cycle: %s", strings.Join(append(path, n), " -> "))
		}
		color[n] = gray
		deps := append([]string(nil), byName[n].Needs...)
		sort.Strings(deps)
		for _, dep := range deps {
			if _, ok := byName[dep]; !ok {
				return fmt.Errorf("node %q needs unknown node %q", n, dep)
			}
			if err := visit(dep, append(path, n)); err != nil {
				return err
			}
		}
		color[n] = black
		order = append(order, n)
		return nil
	}
	for _, n := range names {
		if err := visit(n, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// Compose runs a DAG of packs in topological order on this session, materializing
// each node's labelResults as a `pack_<name>` temp keyspace so downstream nodes can
// `FROM` it. Returns every node's rows (leaf nodes are the higher-level
// correlation/incident output). The temp keyspaces live for the session's lifetime.
func (s *Session) Compose(nodes []ComposeNode) (*ComposeResult, error) {
	order, err := TopoOrder(nodes)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]*ComposeNode, len(nodes))
	for i := range nodes {
		byName[nodes[i].Name] = &nodes[i]
	}
	res := &ComposeResult{Order: order}
	for _, name := range order {
		nd := byName[name]
		cc, cerr := s.MultiQueryCompile(nd.Entries)
		if cerr != nil {
			return nil, fmt.Errorf("compose %q: compile: %v", name, cerr)
		}
		// A node's single-entry pack that fails to parse/plan/convert lands in
		// cc.Rejected and would otherwise run to a silent zero (ISSUE-09). Surface it
		// as a rejected node with its reason rather than an indistinguishable count:0.
		if len(cc.Rejected) > 0 {
			res.Nodes = append(res.Nodes, ComposeNodeResult{
				Node: name, Count: 0, Rejected: true, Reason: cc.Rejected[0].Reason})
			// Materialize an empty keyspace so a downstream FROM resolves (to nothing).
			if merr := s.materializeLabelResults(ComposeKeyspace(name), nil); merr != nil {
				return nil, fmt.Errorf("compose %q: materialize: %v", name, merr)
			}
			continue
		}
		lrs, _, rerr := cc.RunReport()
		if rerr != nil {
			return nil, fmt.Errorf("compose %q: run: %v", name, rerr)
		}
		if merr := s.materializeLabelResults(ComposeKeyspace(name), lrs); merr != nil {
			return nil, fmt.Errorf("compose %q: materialize: %v", name, merr)
		}
		res.Nodes = append(res.Nodes, ComposeNodeResult{Node: name, Count: len(lrs), LabelResults: lrs})
	}
	return res, nil
}

// materializeLabelResults publishes lrs as a temp keyspace of {label,result,
// fingerprint} rows (replacing any prior keyspace of that name).
func (s *Session) materializeLabelResults(keyspace string, lrs []LabelResult) error {
	if s.Store == nil || s.Store.Temp == nil {
		return fmt.Errorf("no temp-keyspace registry on this session")
	}
	heap, err := s.Store.Temp.NewHeap()
	if err != nil {
		return err
	}
	var buf []byte
	for _, lr := range lrs {
		buf = appendComposeRow(buf[:0], lr)
		if e := heap.PushBytes(buf); e != nil { // PushBytes copies, so buf is reusable
			return e
		}
	}
	return s.Store.Temp.Put(keyspace, heap)
}

// appendComposeRow renders a labelResult as the row a downstream pack FROMs:
// {"label":..,"result":..,"fingerprint":..}. A missing result serializes as null.
func appendComposeRow(dst []byte, lr LabelResult) []byte {
	result := lr.Result
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	row := struct {
		Label       string          `json:"label"`
		Result      json.RawMessage `json:"result"`
		Fingerprint string          `json:"fingerprint"`
	}{Label: lr.Label, Result: result, Fingerprint: labelResultFingerprint(lr)}
	b, _ := json.Marshal(row)
	return append(dst, b...)
}

// labelResultFingerprint is the content dedup_key / lineage handle for a
// labelResult: a short hash over (label, result).
func labelResultFingerprint(lr LabelResult) string {
	h := sha256.New()
	h.Write([]byte(lr.Label))
	h.Write([]byte{0})
	h.Write(lr.Result)
	return hex.EncodeToString(h.Sum(nil))[:6]
}
