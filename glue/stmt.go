//  Copyright (c) 2019 Couchbase, Inc.
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

import (
	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/file"
	"github.com/couchbase/query/datastore/system"
	"github.com/couchbase/query/parser/n1ql"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/planner"
	"github.com/couchbase/query/semantics"
	"github.com/couchbase/query/util"
	"github.com/couchbase/query/value"
)

// ParseStatement parses and checks semantics on a N1QL statement.
func ParseStatement(stmt, namespace string, ent bool) (algebra.Statement, error) {
	queryContext := "" // TODO.

	s, err := n1ql.ParseStatement2(stmt, namespace, queryContext)
	if err != nil {
		return nil, err
	}

	txn := false // TODO.

	_, err = s.Accept(semantics.NewSemChecker(ent, s.Type(), txn))
	if err != nil {
		return nil, err
	}

	return s, nil
}

// ------------------------------------------------------------------

// Store represents a datastore/systemstore configuration for
// processing N1QL statements.
type Store struct {
	Datastore       datastore.Datastore
	Systemstore     datastore.Systemstore
	IndexApiVersion int
	FeatureControls uint64
}

// ------------------------------------------------------------------

// InitParser initializes the global n1ql parser with store info.
func (g *Store) InitParser() error {
	ns, err := g.Datastore.NamespaceNames()
	if err != nil {
		return err
	}

	nsm := make(map[string]interface{}, len(ns))
	for i, _ := range ns {
		nsm[ns[i]] = true
	}

	n1ql.SetNamespaces(nsm)

	datastore.SetDatastore(g.Datastore)

	return nil
}

// ------------------------------------------------------------------

// PlanStatement returns a plan.Operator tree for a statement.
func (g *Store) PlanStatement(s algebra.Statement, namespace string,
	namedArgs map[string]value.Value, positionalArgs value.Values) (
	plan.Operator, error) {
	var subquery bool
	var stream bool

	var pc planner.PrepareContext

	planner.NewPrepareContext(&pc,
		"requestId-0",    // requestId
		"queryContext-0", // queryContext
		namedArgs,
		positionalArgs,
		g.IndexApiVersion,
		g.FeatureControls,
		false, // useFts
		false, // useCBO
		nil,   // optimizer
		nil,   // deltaKeyspaces
		nil,   // dsContext
	)

	op, _, err := planner.Build(s, g.Datastore, g.Systemstore,
		namespace, subquery, stream, &pc)

	return op, err
}

// ------------------------------------------------------------------

// FileStore returns a store instance based on a file datastore.
func FileStore(path string) (*Store, error) {
	ds, err := file.NewDatastore(path)
	if err != nil {
		return nil, err
	}

	ss, err := system.NewDatastore(ds)
	if err != nil {
		return nil, err
	}

	return &Store{
		Datastore:       ds,
		Systemstore:     ss,
		IndexApiVersion: datastore.INDEX_API_MAX,
		FeatureControls: util.DEF_N1QL_FEAT_CTRL,
	}, nil
}
