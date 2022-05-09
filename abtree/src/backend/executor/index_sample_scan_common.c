/*-------------------------------------------------------------------------
 *
 * index_sample_scan_common.c
 *	  Common routines to support index/index-only sample scans.
 *
 * IDENTIFICATION
 *	  src/backend/executor/index_sample_scan_common.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/index_sample_scan_common.h"
#include "utils/builtins.h"
#include "utils/sampling.h"

bool tablesample_swr_ask_for_samples_attempted = false;

static double high_rejection_rate_threshold_multiplier = 1000.0;

void
index_sample_scan_init(IndexSampleScanState *node,
					   Expr *repeatable_expr,
					   PlanState *planstate,
					   uint64 sample_size,
					   Expr *sample_size_expr)
{
	if (!sample_size_expr)
	{
		/* constant sample size, use the one cached in the plan. */
		node->isss_SampleSize = sample_size;
		node->isss_SampleSizeExprState = NULL;
	}
	else
	{
		node->isss_SampleSizeExprState =
			ExecInitExpr(sample_size_expr, planstate);
	}

	if (!repeatable_expr)
	{
		node->isss_Seed = random();
		node->isss_RepeatableExprState = NULL;
	}
	else
		node->isss_RepeatableExprState =
			ExecInitExpr(repeatable_expr, planstate);

	node->isss_HighRejectionRateWarningThreshold =
		sample_size * high_rejection_rate_threshold_multiplier;
}

void
index_sample_scan_start(IndexSampleScanState *node,
						ExprContext *econtext)
{
	uint32		seed;
	
	/* We shouldn't restart the sampling unless told so. */
	if (node->isss_RescanForRuntimeKeysOnly) return;

	if (node->isss_SampleSizeExprState) {
		bool			isnull;
		Datum			datum;

		datum = ExecEvalExprSwitchContext(
					node->isss_SampleSizeExprState,
					econtext,
					&isnull);

		if (isnull || DatumGetInt64(datum) < 0)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
					errmsg("TABLESAMPLE SWR() parameter cannot be null or "
						   "negative"));

		node->isss_RemSampleSize = DatumGetInt64(datum);
	}
	else
	{
		node->isss_RemSampleSize = node->isss_SampleSize;
	}
	node->isss_NumIndexAccessAttempted = 0;

	if (node->isss_RepeatableExprState)
	{
		bool			isnull;
		Datum			datum;
		
		datum = ExecEvalExprSwitchContext(
					node->isss_RepeatableExprState,
					econtext,
					&isnull);

		if (isnull)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_TABLESAMPLE_REPEAT),
					errmsg("TABLESAMPLE REPEATABLLE parameter cannot be null"));

		seed = DatumGetUInt32(DirectFunctionCall1(hashfloat8, datum));
	}
	else
	{
		seed = node->isss_Seed;
	}

	sampler_random_init_state((long) seed, node->isss_RandState);

	/* 
	 * Reset the bit here so that next call from ExecIndexOnlyScan() will
	 * not re-initialize the sampler.
	 */
	node->isss_RescanForRuntimeKeysOnly = false;
}

