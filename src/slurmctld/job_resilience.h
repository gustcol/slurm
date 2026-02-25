/*****************************************************************************\
 *  job_resilience.h - adaptive job resilience on node failure
 *
 *  When a node fails during a running job, instead of killing the job,
 *  the allocation is shrunk to surviving nodes and the job continues.
 *  As nodes recover, the allocation can be expanded back to its original
 *  size. This behavior is gated by the ADAPTIVE_RESILIENCE bit flag and
 *  a configurable minimum cluster health threshold.
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

#ifndef _JOB_RESILIENCE_H
#define _JOB_RESILIENCE_H

#include "src/slurmctld/slurmctld.h"

/*
 * Return current cluster health as an integer percentage 0-100.
 * Uses up_node_bitmap and active_node_record_count.
 * Caller must hold NODE_LOCK (READ_LOCK).
 */
extern int cluster_health_pct(void);

/*
 * Return true if job_ptr is eligible for adaptive resilience handling
 * on this node failure event. Checks ADAPTIVE_RESILIENCE flag, node count,
 * batch mode, and cluster health threshold.
 */
extern bool job_resilience_eligible(job_record_t *job_ptr);

/*
 * Handle node failure for a resilience-eligible job by shrinking the
 * allocation to surviving nodes. Records original node count for
 * potential elastic recovery.
 *
 * Caller must hold JOB_LOCK (WRITE_LOCK) and NODE_LOCK (WRITE_LOCK).
 *
 * Returns SLURM_SUCCESS if the job was successfully shrunk, or
 * SLURM_ERROR if the job cannot survive the node loss (e.g., last node).
 */
extern int job_resilience_suspend(job_record_t *job_ptr,
				  node_record_t *node_ptr);

/*
 * Called from the node recovery path when a node transitions back to
 * available state. Scans resilience-active jobs to see if any can
 * reclaim the recovered node and expand their allocation.
 *
 * Caller must hold JOB_LOCK (WRITE_LOCK) and NODE_LOCK (WRITE_LOCK).
 */
extern void job_resilience_node_recovered(node_record_t *node_ptr);

#endif /* _JOB_RESILIENCE_H */
