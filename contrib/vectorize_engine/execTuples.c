/*-------------------------------------------------------------------------
 *
 * execTuples.c
 *	  Routines dealing with TupleTableSlots.  These are used for resource
 *	  management associated with tuples (eg, releasing buffer pins for
 *	  tuples in disk buffers, or freeing the memory occupied by transient
 *	  tuples).  Slots also provide access abstraction that lets us implement
 *	  "virtual" tuples to reduce data-copying overhead.
 *
 *	  Routines dealing with the type information for tuples. Currently,
 *	  the type information for a tuple is an array of FormData_pg_attribute.
 *	  This information is needed by routines manipulating tuples
 *	  (getattribute, formtuple, etc.).
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execTuples.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *
 *	 SLOT CREATION/DESTRUCTION
 *		MakeTupleTableSlot		- create an empty slot
 *		ExecAllocTableSlot		- create a slot within a tuple table
 *		ExecResetTupleTable		- clear and optionally delete a tuple table
 *		MakeSingleTupleTableSlot - make a standalone slot, set its descriptor
 *		ExecDropSingleTupleTableSlot - destroy a standalone slot
 *
 *	 SLOT ACCESSORS
 *		ExecSetSlotDescriptor	- set a slot's tuple descriptor
 *		ExecStoreTuple			- store a physical tuple in the slot
 *		ExecStoreMinimalTuple	- store a minimal physical tuple in the slot
 *		ExecClearTuple			- clear contents of a slot
 *		ExecStoreVirtualTuple	- mark slot as containing a virtual tuple
 *		ExecCopySlotTuple		- build a physical tuple from a slot
 *		ExecCopySlotMinimalTuple - build a minimal physical tuple from a slot
 *		ExecMaterializeSlot		- convert virtual to physical storage
 *		ExecCopySlot			- copy one slot's contents to another
 *
 *	 CONVENIENCE INITIALIZATION ROUTINES
 *		ExecInitResultTupleSlot    \	convenience routines to initialize
 *		ExecInitScanTupleSlot		\	the various tuple slots for nodes
 *		ExecInitExtraTupleSlot		/	which store copies of tuples.
 *		ExecInitNullTupleSlot	   /
 *
 *	 Routines that probably belong somewhere else:
 *		ExecTypeFromTL			- form a TupleDesc from a target list
 *
 *	 EXAMPLE OF HOW TABLE ROUTINES WORK
 *		Suppose we have a query such as SELECT emp.name FROM emp and we have
 *		a single SeqScan node in the query plan.
 *
 *		At ExecutorStart()
 *		----------------
 *		- ExecInitSeqScan() calls ExecInitScanTupleSlot() and
 *		  ExecInitResultTupleSlot() to construct TupleTableSlots
 *		  for the tuples returned by the access methods and the
 *		  tuples resulting from performing target list projections.
 *
 *		During ExecutorRun()
 *		----------------
 *		- SeqNext() calls ExecStoreTuple() to place the tuple returned
 *		  by the access methods into the scan tuple slot.
 *
 *		- ExecSeqScan() calls ExecStoreTuple() to take the result
 *		  tuple from ExecProject() and place it into the result tuple slot.
 *
 *		- ExecutePlan() calls the output function.
 *
 *		The important thing to watch in the executor code is how pointers
 *		to the slots containing tuples are passed instead of the tuples
 *		themselves.  This facilitates the communication of related information
 *		(such as whether or not a tuple should be pfreed, what buffer contains
 *		this tuple, the tuple's tuple descriptor, etc).  It also allows us
 *		to avoid physically constructing projection tuples in many cases.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tuptoaster.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "execTuples.h"
#include "executor/vtype.h"
#include "utils.h"
#include "vectorTupleSlot.h"

/* static vectorized functions */

/*
 * TupleTableSlotOps implementation for VectorTupleTableSlot.
 */
const TupleTableSlotOps TTSOpsVector;

static void
tts_vector_init(TupleTableSlot *slot)
{
	VectorTupleSlot		*vslot;

	slot->tts_flags |= TTS_FLAG_EMPTY;

	/* vectorized fields */
	vslot = (VectorTupleSlot*)slot;
	vslot->dim = 0;
	vslot->bufnum = 0;
	memset(vslot->tts_buffers, InvalidBuffer, sizeof(vslot->tts_buffers));
	memset(vslot->tts_tuples, 0, sizeof(vslot->tts_tuples));
	/* all tuples should be skipped in initialization */
	memset(vslot->skip, true, sizeof(vslot->skip));

	InitializeVectorSlotColumn(vslot);
}

static void
tts_vector_release(TupleTableSlot *slot)
{
}

static void
tts_vector_clear(TupleTableSlot *slot)
{
	int				i;
	vtype			*column;
	VectorTupleSlot *vslot;
	/*
	 * sanity checks
	 */
	Assert(slot != NULL);

	if (TTS_EMPTY(slot))
		return;

	vslot = (VectorTupleSlot *)slot;

	slot->tts_flags &=~ TTS_FLAG_SHOULDFREE;

	/*
	 * Mark it empty.
	 */
	slot->tts_flags |= TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;

	/* vectorize part  */
	for(i = 0; i < vslot->bufnum; i++)
	{
		if(BufferIsValid(vslot->tts_buffers[i]))
		{
			ReleaseBuffer(vslot->tts_buffers[i]);
			vslot->tts_buffers[i] = InvalidBuffer;
		}
	}
	vslot->dim = 0;
	vslot->bufnum = 0;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		column = (vtype *)DatumGetPointer(slot->tts_values[i]);
		column->dim = 0;
	}

	memset(vslot->skip, true, sizeof(vslot->skip));
}

static void
tts_vector_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(PANIC, "getsomeattrs is not required to be called on a vector tuple table slot");
}

static Datum
tts_vector_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot retrieve a system column in this context")));

	return 0;					/* silence compiler warnings */
}

static void
tts_vector_materialize(TupleTableSlot *slot)
{
	elog(ERROR, "meterialize is not supported on vector tuple slot");
}

static void
tts_vector_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	elog(ERROR, "copy is not supported on vector tuple slot");
}

static HeapTuple
tts_vector_copy_heap_tuple(TupleTableSlot *slot)
{
	Datum 			*values;
	bool			*isnull;
	HeapTuple 		htup;

	Assert(!TTS_EMPTY(slot));

	values = palloc(sizeof(Datum) * slot->tts_tupleDescriptor->natts);
	isnull = palloc(sizeof(bool) * slot->tts_tupleDescriptor->natts);

	for (int i = 0; i < slot->tts_tupleDescriptor->natts; ++i)
	{
		vtype		*column;

		column = (vtype *) DatumGetPointer(slot->tts_values[i]);
		values[i] = column->values[0];
		isnull[i] = column->isnull[0];
	}

	htup = heap_form_tuple(slot->tts_tupleDescriptor, values, isnull);
	pfree(values);
	pfree(isnull);

	return htup;
}

static MinimalTuple
tts_vector_copy_minimal_tuple(TupleTableSlot *slot)
{
	return NULL;
}

const TupleTableSlotOps TTSOpsVector = {
	.base_slot_size = sizeof(VectorTupleSlot),
	.init = tts_vector_init,
	.release = tts_vector_release,
	.clear = tts_vector_clear,
	.getsomeattrs = tts_vector_getsomeattrs,
	.getsysattr = tts_vector_getsysattr,
	.materialize = tts_vector_materialize,
	.copyslot = tts_vector_copyslot,

	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_vector_copy_heap_tuple,
	.copy_minimal_tuple = tts_vector_copy_minimal_tuple
};
