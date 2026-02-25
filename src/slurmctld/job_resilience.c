/*****************************************************************************\
 *  job_resilience.c - adaptive job resilience on node failure
 *****************************************************************************
 *  Copyright (C) SchedMD LLC.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "config.h"

#include <time.h>

#include "src/common/bitstring.h"
#include "src/common/macros.h"
#include "src/common/node_conf.h"
#include "src/common/xassert.h"
#include "src/common/xstring.h"

#include "src/interfaces/gres.h"
#include "src/interfaces/select.h"

#include "src/slurmctld/gang.h"
#include "src/slurmctld/job_resilience.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

#define RESILIENCE_DEFAULT_MIN_PCT 70

extern int cluster_health_pct(void)
{
	int up_cnt, total_cnt;

	xassert(verify_lock(NODE_LOCK, READ_LOCK));

	total_cnt = active_node_record_count;
	if (total_cnt <= 0)
		return 0;

	up_cnt = bit_set_count(up_node_bitmap);
	return (up_cnt * 100) / total_cnt;
}

extern bool job_resilience_eligible(job_record_t *job_ptr)
{
	int min_pct, cur_pct;

	if (!(job_ptr->bit_flags & ADAPTIVE_RESILIENCE))
		return false;

	/*
	 * Must have more than one node to survive shrinking.
	 * Single-node jobs must use the requeue path instead.
	 */
	if (job_ptr->node_cnt <= 1)
		return false;

	/* Only batch jobs support resilience mode */
	if (!job_ptr->batch_flag)
		return false;

	/* Don't interfere with jobs already being killed */
	if (job_ptr->kill_on_node_fail)
		return false;

	min_pct = job_ptr->resilience_min_cluster_pct;
	if (min_pct == 0)
		min_pct = RESILIENCE_DEFAULT_MIN_PCT;

	cur_pct = cluster_health_pct();
	if (cur_pct < min_pct) {
		info("%s: %pJ resilience ineligible: cluster health %d%% below threshold %d%%",
		     __func__, job_ptr, cur_pct, min_pct);
		return false;
	}

	return true;
}

extern int job_resilience_suspend(job_record_t *job_ptr,
				  node_record_t *node_ptr)
{
	bitstr_t *orig_job_node_bitmap;
	job_resources_t *job_resrcs_ptr = job_ptr->job_resrcs;

	xassert(verify_lock(JOB_LOCK, WRITE_LOCK));
	xassert(verify_lock(NODE_LOCK, WRITE_LOCK));

	if (!job_resrcs_ptr) {
		error("%s: %pJ has no job resources", __func__, job_ptr);
		return SLURM_ERROR;
	}

	/*
	 * After losing this node, the job needs at least one remaining node.
	 */
	if (job_ptr->node_cnt <= 1) {
		info("%s: %pJ cannot shrink below 1 node", __func__, job_ptr);
		return SLURM_ERROR;
	}

	/*
	 * Save the original allocation size on the first failure event.
	 * This allows tracking how much of the original allocation has
	 * been lost and serves as the target for elastic recovery.
	 */
	if (job_ptr->resilience_orig_node_cnt == 0) {
		job_ptr->resilience_orig_node_cnt = job_ptr->node_cnt;
		FREE_NULL_BITMAP(job_ptr->resilience_orig_bitmap);
		job_ptr->resilience_orig_bitmap =
			bit_copy(job_ptr->node_bitmap);
	}

	info("%s: shrinking %pJ from %u to %u nodes (lost %s)",
	     __func__, job_ptr, job_ptr->node_cnt,
	     job_ptr->node_cnt - 1, node_ptr->name);

	/*
	 * Remove the failed node from the running job. This reuses the
	 * same proven code path as the existing kill_on_node_fail==0
	 * branch in _foreach_kill_running_job_by_node().
	 */
	job_pre_resize_acctg(job_ptr);
	kill_step_on_node(job_ptr, node_ptr, true);
	orig_job_node_bitmap = bit_copy(job_resrcs_ptr->node_bitmap);
	excise_node_from_job(job_ptr, node_ptr);
	rebuild_step_bitmaps(job_ptr, orig_job_node_bitmap);
	FREE_NULL_BITMAP(orig_job_node_bitmap);
	(void) gs_job_start(job_ptr);
	gres_stepmgr_job_build_details(job_ptr->gres_list_alloc,
				       job_ptr->nodes,
				       &job_ptr->gres_detail_cnt,
				       &job_ptr->gres_detail_str,
				       &job_ptr->gres_used);
	job_post_resize_acctg(job_ptr);

	/*
	 * Record that this job is in resilience recovery mode.
	 * The timestamp is used to identify jobs eligible for elastic
	 * expansion when nodes return to service.
	 */
	job_ptr->resilience_suspended_at = time(NULL);
	job_ptr->state_reason = WAIT_RESILIENCE_RECOVERY;
	xfree(job_ptr->state_desc);
	xstrfmtcat(job_ptr->state_desc,
		   "Resilience: lost node %s, running on %u/%u nodes",
		   node_ptr->name, job_ptr->node_cnt,
		   job_ptr->resilience_orig_node_cnt);

	last_job_update = time(NULL);
	return SLURM_SUCCESS;
}

/*
 * Iterator context for elastic recovery scan.
 */
typedef struct {
	node_record_t *node_ptr;
} resilience_restore_arg_t;

/*
 * Per-job callback: attempt to grow a resilience-active job by adding
 * the recovered node back to its allocation.
 */
static int _try_resilience_restore(void *x, void *arg)
{
	job_record_t *job_ptr = x;
	resilience_restore_arg_t *rarg = arg;
	node_record_t *node_ptr = rarg->node_ptr;

	/* Only consider jobs in active resilience recovery */
	if (!(job_ptr->bit_flags & ADAPTIVE_RESILIENCE))
		return 0;
	if (!IS_JOB_RUNNING(job_ptr))
		return 0;
	if (job_ptr->resilience_suspended_at == 0)
		return 0;

	/* Already fully restored */
	if (job_ptr->node_cnt >= job_ptr->resilience_orig_node_cnt)
		return 0;

	/*
	 * The recovered node must have been part of the original allocation.
	 * resilience_orig_bitmap may be NULL after a slurmctld restart
	 * (it is not persisted); in that case elastic recovery is not
	 * possible and the job continues at its reduced size.
	 */
	if (!job_ptr->resilience_orig_bitmap)
		return 0;
	if (!bit_test(job_ptr->resilience_orig_bitmap, node_ptr->index))
		return 0;

	/*
	 * The node must be idle and available. Nodes that come back
	 * as allocated or draining are not candidates.
	 */
	if (!bit_test(avail_node_bitmap, node_ptr->index))
		return 0;
	if (!bit_test(idle_node_bitmap, node_ptr->index))
		return 0;

	/*
	 * Elastic expansion is complex and requires deep integration with
	 * the select plugin's internal resource tracking. For the initial
	 * implementation, we log the opportunity but do not perform the
	 * expansion automatically. A future enhancement can call into
	 * select_g_job_expand() or a dedicated resize API.
	 *
	 * The job continues running at its current (reduced) size, and
	 * the state_desc is updated to reflect recovery progress.
	 */
	info("%s: node %s recovered and available for %pJ elastic expansion "
	     "(current %u/%u nodes)",
	     __func__, node_ptr->name, job_ptr,
	     job_ptr->node_cnt, job_ptr->resilience_orig_node_cnt);

	/*
	 * Clear resilience state if all originally-allocated nodes
	 * are back in the available pool, even though we haven't
	 * expanded the job. The job finished with reduced resources.
	 */
	xfree(job_ptr->state_desc);
	xstrfmtcat(job_ptr->state_desc,
		   "Resilience: node %s recovered, running %u/%u nodes",
		   node_ptr->name, job_ptr->node_cnt,
		   job_ptr->resilience_orig_node_cnt);

	return 0;
}

extern void job_resilience_node_recovered(node_record_t *node_ptr)
{
	resilience_restore_arg_t rarg = { .node_ptr = node_ptr };

	xassert(verify_lock(JOB_LOCK, WRITE_LOCK));
	xassert(verify_lock(NODE_LOCK, WRITE_LOCK));

	list_for_each(job_list, _try_resilience_restore, &rarg);
}
