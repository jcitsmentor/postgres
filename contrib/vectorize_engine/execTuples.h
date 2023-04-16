#ifndef VECTOR_ENGINE_EXEC_TUPLES_H
#define VECTOR_ENGINE_EXEC_TUPLES_H

#include "postgres.h"

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "executor/tuptable.h"
#include "storage/buf.h"
#include "executor/vtype.h"

/*
 * prototypes from functions in execTuples.c
 */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsVector;


extern void VExecInitResultTupleSlot(EState *estate, PlanState *planstate);
extern void VExecInitScanTupleSlot(EState *estate, ScanState *scanstate,
								   TupleDesc tupledesc, const TupleTableSlotOps *tts_ops);
extern TupleTableSlot *VExecInitExtraTupleSlot(EState *estate);
#endif
