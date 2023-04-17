#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/xml.h"

#include "executor.h"
#include "execTuples.h"
#include "vectorTupleSlot.h"
#include "executor/vtype.h"

/* ----------------------------------------------------------------
 *		ExecQual
 *
 *		Evaluates a conjunctive boolean expression (qual list) and
 *		returns true iff none of the subexpressions are false.
 *		(We also return true if the list is empty.)
 *
 *	If some of the subexpressions yield NULL but none yield FALSE,
 *	then the result of the conjunction is NULL (ie, unknown)
 *	according to three-valued boolean logic.  In this case,
 *	we return the value specified by the "resultForNull" parameter.
 *
 *	Callers evaluating WHERE clauses should pass resultForNull=FALSE,
 *	since SQL specifies that tuples with null WHERE results do not
 *	get selected.  On the other hand, callers evaluating constraint
 *	conditions should pass resultForNull=TRUE, since SQL also specifies
 *	that NULL constraint conditions are not failures.
 *
 *	NOTE: it would not be correct to use this routine to evaluate an
 *	AND subclause of a boolean expression; for that purpose, a NULL
 *	result must be returned as NULL so that it can be properly treated
 *	in the next higher operator (cf. ExecEvalAnd and ExecEvalOr).
 *	This routine is only used in contexts where a complete expression
 *	is being evaluated and we know that NULL can be treated the same
 *	as one boolean result or the other.
 *
 * ----------------------------------------------------------------
 */
bool
VExecScanQual(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;
	int                             row;
	TupleTableSlot  *slot;
	VectorTupleSlot *vslot;
	vbool           *expr_val_bools;

	state->is_vector = true;

	/* short-circuit (here and in ExecInitQual) for empty restriction list */
	if (state == NULL)
		return true;

	/* verify that expression was compiled using ExecInitQual */
	Assert(state->flags & EEO_FLAG_IS_QUAL);

	slot = econtext->ecxt_scantuple;
	vslot = (VectorTupleSlot *)slot;

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	expr_val_bools = (vbool *) DatumGetPointer(ret);

	for (row = 0; row < BATCHSIZE; ++row)
	{
		if((!expr_val_bools->isnull[row]) &&
			!DatumGetBool(expr_val_bools->values[row]) &&
			!vslot->skip[row])
			vslot->skip[row] = true;
	}

	state->boolvalue = 0;

	/* EEOP_QUAL should never return NULL */
	Assert(!isnull);

	for(row = 0; row < BATCHSIZE; row++)
	{
		if (!vslot->skip[row])
			return true;
	}

	return false;
}

