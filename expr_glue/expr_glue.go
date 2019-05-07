// The expr_glue package leverages the existing
// couchbase.com/query/expression package to implement expressions for
// backwards compatibility.

package expr_glue

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

var RHMapSizeSmall = 97

// ExprGlue parses and evaluates a N1QL expression string using the
// query/expression package, for full backwards compatibility at the
// cost of performance.
func ExprGlue(vars *base.Vars, labels base.Labels,
	params []interface{}, path string) (exprFunc base.ExprFunc) {
	exprStr := params[0].(string)

	var expr expression.Expression
	var conv *Conv
	var err error
	var errSent error

	expr, err = parser.Parse(exprStr)
	if err == nil {
		conv, err = NewConv(labels, RHMapSizeSmall)
	}

	exprGlueContext := &ExprGlueContext{NowTime: vars.Ctx.Now}

	return func(vals base.Vals, yieldErr base.YieldErr) (val base.Val) {
		if err != nil {
			if errSent == nil {
				errSent = err

				yieldErr(err)
			}

			return nil
		}

		v, err := conv.Convert(vals)
		if err != nil {
			return base.ValNull // TODO: Is this right?
		}

		vResult, err := expr.Evaluate(v, exprGlueContext)
		if err != nil {
			return base.ValNull // TODO: Is this right?
		}

		jResult, err := vResult.MarshalJSON()
		if err != nil {
			return base.ValNull // TODO: Is this right?
		}

		return base.Val(jResult)
	}
}

// --------------------------------------------------------

// ExprGlueContext implements query/expression.Context interface.
type ExprGlueContext struct {
	NowTime time.Time
}

func (e *ExprGlueContext) Now() time.Time {
	return e.NowTime
}

func (e *ExprGlueContext) AuthenticatedUsers() []string {
	return nil // TODO.
}

func (e *ExprGlueContext) DatastoreVersion() string {
	return "" // TODO.
}

// --------------------------------------------------------

// Conv represents reusable state to convert base.Vals to value.Value.
type Conv struct {
	Labels base.Labels
	Paths  [][]string // The len(Paths) == len(Labels).
}

func NewConv(labels base.Labels, size int) (*Conv, error) {
	var paths [][]string

	for _, label := range labels {
		var path []string

		if len(label) > 0 && label[0] == '.' {
			err := json.Unmarshal([]byte(label[1:]), &path)
			if err != nil {
				return nil, err
			}
		}

		paths = append(paths, path)
	}

	return &Conv{Labels: labels, Paths: paths}, nil
}

// Convert merges the vals into a single value.Value, based on the
// directives provided in ValsToValue.Labels.
func (s *Conv) Convert(vals base.Vals) (value.Value, error) {
	if len(s.Labels) != len(vals) {
		return nil, errors.New("Conv, Labels.len != vals.len")
	}

	var v value.Value // The result of vals flattened or merged.

OUTER:
	for i, label := range s.Labels {
		switch label[0] {
		case '=': // The label denotes that vals[i] is a BINARY value.
			if v != nil {
				return nil, errors.New("Conv, v non-nil on '='")
			}

			v = value.NewBinaryValue(vals[i])

		case '.': // Label is a path into v of where to set vals[i].
			if label == "." {
				if v != nil {
					return nil, errors.New("Conv, v non-nil on '.'")
				}

				v = value.NewParsedValue(vals[i], false)

				continue OUTER
			}

			if v == nil {
				v = value.NewValue(map[string]interface{}{})
			}

			subObj := v

			path := s.Paths[i]

			for j := 0; j < len(path)-1; j++ {
				subObjNext, ok := subObj.Field(path[j])
				if ok {
					subObj = subObjNext
				} else {
					m := map[string]interface{}{}

					err := subObj.SetField(path[j], m)
					if err != nil {
						return nil, err
					}

					subObj = value.NewValue(m)
				}
			}

			var iv interface{}

			err := json.Unmarshal(vals[i], &iv)
			if err != nil {
				return nil, err
			}

			err = subObj.SetField(path[len(path)-1], iv)
			if err != nil {
				return nil, err
			}

		case '^': // The label is an attachment name for vals[i].
			var iv interface{}

			err := json.Unmarshal(vals[i], &iv)
			if err != nil {
				return nil, err
			}

			av, ok := v.(value.AnnotatedValue)
			if !ok {
				av = value.NewAnnotatedValue(v)
			}

			if label[1:] == "id" {
				av.SetId(iv)
			} else {
				av.SetAttachment(label[1:], iv)
			}

			v = av

		default:
			return nil, errors.New("Conv, unknown label kind")
		}
	}

	return v, nil
}
