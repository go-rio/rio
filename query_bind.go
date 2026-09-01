package rio

import "fmt"

// prepareQueryState validates and binds a query before any driver call.
func prepareQueryState[T any](d Dialect, s *queryState, execArgs []any) (*plan, queryState, error) {
	p, err := planOf[T]()
	if err != nil {
		return nil, queryState{}, err
	}
	if err := validateQueryState(p, s); err != nil {
		return nil, queryState{}, err
	}
	bound, err := bindQueryState(d, p, s, execArgs)
	if err != nil {
		return nil, queryState{}, err
	}
	return p, bound, nil
}

// bindQueryState returns an execution-local state without mutating s or
// execArgs. A keyed state consumes the primary-key values first.
func bindQueryState(d Dialect, p *plan, s *queryState, execArgs []any) (queryState, error) {
	out := *s
	argIndex := 0
	if s.keyed {
		argIndex = len(p.pks)
		out.keyArgs = execArgs[:argIndex:argIndex]
	}

	bind := func(clause string, src []cond) ([]cond, error) {
		dst := src
		cloned := false
		for i, c := range src {
			var holes int
			_, _, _ = rebindCount(d.lexer(), c.expr, &holes)
			inline := len(c.args)
			switch {
			case inline == holes:
				continue
			case inline != 0:
				return nil, fmt.Errorf(
					"rio: %s(%q) has %d placeholder(s) but %d inline argument(s) under the %s dialect",
					clause,
					c.expr,
					holes,
					inline,
					d.name(),
				)
			case argIndex+holes > len(execArgs):
				return nil, fmt.Errorf(
					"rio: query needs at least %d deferred argument(s), got %d (at %s(%q))",
					argIndex+holes,
					len(execArgs),
					clause,
					c.expr,
				)
			default:
				if !cloned {
					dst = append([]cond(nil), src...)
					cloned = true
				}
				end := argIndex + holes
				dst[i] = cond{expr: c.expr, args: execArgs[argIndex:end:end]}
				argIndex = end
			}
		}
		return dst, nil
	}

	var err error
	if out.wheres, err = bind("Where", s.wheres); err != nil {
		return queryState{}, err
	}
	if out.havings, err = bind("Having", s.havings); err != nil {
		return queryState{}, err
	}
	if argIndex != len(execArgs) {
		return queryState{}, fmt.Errorf("rio: query takes %d deferred argument(s), got %d", argIndex, len(execArgs))
	}
	return out, nil
}
