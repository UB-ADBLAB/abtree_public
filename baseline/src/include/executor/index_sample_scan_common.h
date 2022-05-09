/*-------------------------------------------------------------------------
 *
 * index_sample_scan_common.h
 *
 * src/include/executor/index_sample_scan_common.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_SAMPLE_SCAN_COMMON_H
#define INDEX_SAMPLE_SCAN_COMMON_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "utils/float.h"
#include "utils/sampling.h"

extern bool tablesample_swr_ask_for_samples_attempted;

/*
 * Increment and check the number of index accesses we have made for the
 * warning of the high rejection rate.
 */
static inline void
index_sample_scan_check_for_high_rejection_rate(IndexSampleScanState *node)
{
	if (++node->isss_NumIndexAccessAttempted >
		node->isss_HighRejectionRateWarningThreshold)
	{
		ereport(WARNING,
				(errcode(ERRCODE_WARNING_HIGH_SAMPLING_REJECTION_RATE),
				 errmsg("The rejection rate of tablesample swr is higher than "
						"99.9%%. It could because the range is empty. Consider "
						"aborting and retry the query without tablesample "
						"swr.")));

		/* 
		 * Setting the threshold to +inf so that we will only emit one
		 * warning for one plan.
		 *
		 * However, get_float8_infinity() is a static inline function while
		 * this is not. We copy its function body here.
		 */
		node->isss_HighRejectionRateWarningThreshold = get_float8_infinity();
	}
}

static inline double
index_sample_scan_next_random_number(IndexSampleScanState *node)
{
	return sampler_random_fract(node->isss_RandState);
}

static inline void
index_sample_scan_got_new_sample(IndexSampleScanState *node)
{
	--node->isss_RemSampleSize;
}

static inline void
index_sample_scan_new_sample_rejected(IndexSampleScanState *node)
{
	++node->isss_RemSampleSize;
}

static inline bool
index_sample_scan_no_more_samples(IndexSampleScanState *node)
{
	if (node->isss_RemSampleSize == 0) {
		if (tablesample_swr_ask_for_samples_attempted)
			ereport(INFO, (errmsg("N=" UINT64_FORMAT,
								  node->isss_NumIndexAccessAttempted)));
		return true;
	}
	return false;
}

static inline void
index_sample_scan_mark_rescan_for_runtime_keys_only(IndexSampleScanState *node)
{
	node->isss_RescanForRuntimeKeysOnly = true;
}

extern void index_sample_scan_init(IndexSampleScanState *node,
								   Expr *repeatable_expr,
								   PlanState *planstate,
								   uint64 sample_size,
								   Expr *sample_size_expr);
extern void index_sample_scan_start(IndexSampleScanState *node,
									ExprContext *econtext);

#endif						/* INDEX_SAMPLE_SCAN_COMMON_H */
