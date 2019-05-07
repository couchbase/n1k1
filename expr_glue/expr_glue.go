// The expr_glue package leverages the existing
// couchbase.com/query/expression package to implement expressions for
// backwards compatibility.

package expr_glue

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

// ExprGlue parses and evaluates a N1QL expression string using the
// query/expression package, for full backwards compatibility at the
// cost of performance.
func ExprGlue(vars *base.Vars, labels base.Labels,
	params []interface{}, path string) (exprFunc base.ExprFunc) {
	exprStr := params[0].(string)

	var cv *ConvertVals

	expr, err := parser.Parse(exprStr)
	if err == nil {
		cv, err = NewConvertVals(labels)
	}

	if err != nil {
		return func(vals base.Vals, yieldErr base.YieldErr) (val base.Val) {
			yieldErr(err)
			return base.ValMissing
		}
	}

	exprGlueContext := &ExprGlueContext{NowTime: vars.Ctx.Now}

	return func(vals base.Vals, yieldErr base.YieldErr) (val base.Val) {
		v, err := cv.Convert(vals)
		if err == nil {
			yieldErr(err)
			return base.ValMissing
		}

		vResult, err := expr.Evaluate(v, exprGlueContext)
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		jResult, err := vResult.MarshalJSON()
		if err != nil {
			yieldErr(err)
			return base.ValMissing
		}

		return base.Val(jResult)
	}
}

// --------------------------------------------------------

// ConvertVals is able to convert base.Vals to value.Value based on
// the directives provided by the Labels.
type ConvertVals struct {
	Labels     base.Labels
	LabelPaths [][]string // The len(LabelPaths) == len(Labels).
}

func NewConvertVals(labels base.Labels) (*ConvertVals, error) {
	var paths [][]string

	for _, label := range labels {
		var path []string

		// Ex label: `.["address","city"]`.
		if len(label) > 0 && label[0] == '.' {
			err := json.Unmarshal([]byte(label[1:]), &path)
			if err != nil {
				return nil, err
			}
		}

		paths = append(paths, path)
	}

	return &ConvertVals{Labels: labels, LabelPaths: paths}, nil
}

// Convert merges the vals into a single value.Value, based on the
// directives provided in ValsToValue.Labels.
func (s *ConvertVals) Convert(vals base.Vals) (value.Value, error) {
	if len(s.Labels) != len(vals) {
		return nil, errors.New("ConvertVals, Labels.len != vals.len")
	}

	var v value.Value // The result of the merged vals.

OUTER:
	for i, label := range s.Labels {
		switch label[0] {
		case '=': // The label denotes that vals[i] is a BINARY value.
			if v != nil {
				return nil, errors.New("ConvertVals, v non-nil on '='")
			}

			v = value.NewBinaryValue(vals[i])

			// Continue loop as remaining labels might be annotations.

		case '.': // Label is a path into v of where to set vals[i].
			if label == "." {
				if v != nil {
					return nil, errors.New("ConvertVals, v non-nil on '.'")
				}

				v = value.NewParsedValue(vals[i], false)

				continue OUTER
			}

			if v == nil {
				v = value.NewValue(map[string]interface{}{})
			}

			subObj := v

			path := s.LabelPaths[i]

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
			return nil, errors.New("ConvertVals, unknown label kind")
		}
	}

	return v, nil
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
