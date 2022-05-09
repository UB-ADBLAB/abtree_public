/*-------------------------------------------------------------------------
 *
 * swr.c
 *	  support routines for SWR (sampling with replacement) tablesample method
 *
 * This will use abtree if available.
 *
 * IDENTIFICATION
 *	  src/backend/access/tablesample/swr.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tsmapi.h"
#include "catalog/pg_type.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"

static void swr_samplescangetsamplesize(PlannerInfo *root,
										RelOptInfo *baserel,
										List *paramexprs,
										BlockNumber *pages,
										double *tuples);

Datum
tsm_swr_handler(PG_FUNCTION_ARGS)
{
	TsmRoutine *tsm = makeNode(TsmRoutine);

	tsm->parameterTypes = list_make1_oid(INT8OID);
	tsm->repeatable_across_queries = false;
	tsm->repeatable_across_scans = false;
	tsm->indexsamplescan = true;
	
	/* 
	 * This returns a dummy data structure because it should be translated into
	 * a IndexScan instead of a SampleScan.
	 */
	tsm->SampleScanGetSampleSize = swr_samplescangetsamplesize;
	tsm->InitSampleScan = NULL;
	tsm->BeginSampleScan = NULL;
	tsm->NextSampleBlock = NULL;
	tsm->NextSampleTuple = NULL;
	tsm->EndSampleScan = NULL;

	return PointerGetDatum(tsm);
}

/* Shouldn't be called but we leave it here for reference for now. */
static void
swr_samplescangetsamplesize(PlannerInfo *root,
							RelOptInfo *baserel,
							List *paramexprs,
							BlockNumber *pages,
							double *tuples)
{
	Node	*num_rows_node;
	int64	num_rows = 0;

	num_rows_node = (Node *) linitial(paramexprs);
	num_rows_node = estimate_expression_value(root, num_rows_node);

	if (IsA(num_rows_node, Const) && !((Const *) num_rows_node)->constisnull)
	{
		num_rows = DatumGetInt64(((Const *) num_rows_node)->constvalue);
		/* Invalid value, will be set to default. */
		if (num_rows < 0)
			num_rows = 0;
	}

	if (num_rows == 0)
	{
		num_rows = 1000;
	}
	
	/* 
	 * We'll access at least one page for each of the sample, although some of
	 * them might be duplicates. That doesn't matter because they are always
	 * random access to the heap file, and we pay the I/O cost anyway unless
	 * the heap file fits in memory.
	 */
	*pages = (double) num_rows;

	*tuples = (double) num_rows;
}

