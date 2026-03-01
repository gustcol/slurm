/*****************************************************************************\
 *  job_submit_resilience.c - Opt-in gate for adaptive job resilience.
 *
 *  Users enable resilience via: sbatch --comment="resilience[=PCT]"
 *  where PCT is an optional cluster health threshold (1-100, default 70).
 *
 *  Admins may also enable per-partition via SchedulerParameters:
 *    SchedulerParameters=resilience_partition=gpu,batch
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

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"
#include "src/common/xstring.h"
#include "src/slurmctld/slurmctld.h"

const char plugin_name[] = "Job submit adaptive resilience plugin";
const char plugin_type[] = "job_submit/resilience";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

/*
 * Check whether the target partition has resilience enabled via
 * SchedulerParameters=resilience_partition=<name>[,<name>...]
 */
static bool _partition_has_resilience(const char *part_name)
{
	char *param, *list, *tok, *save_ptr = NULL;
	char *end;
	bool found = false;

	if (!part_name || !slurm_conf.sched_params)
		return false;

	param = xstrcasestr(slurm_conf.sched_params, "resilience_partition=");
	if (!param)
		return false;

	param += strlen("resilience_partition=");
	list = xstrdup(param);
	end = strpbrk(list, " \t");
	if (end)
		*end = '\0';

	tok = strtok_r(list, ",", &save_ptr);
	while (tok) {
		if (!xstrcasecmp(tok, part_name)) {
			found = true;
			break;
		}
		tok = strtok_r(NULL, ",", &save_ptr);
	}
	xfree(list);
	return found;
}

/*
 * Parse the comment field for resilience opt-in.
 * Supported format: --comment="resilience" or --comment="resilience=80"
 */
static void _parse_resilience_comment(job_desc_msg_t *job_desc)
{
	char *ptr;
	int pct = 0;

	if (!job_desc->comment)
		return;

	ptr = xstrcasestr(job_desc->comment, "resilience");
	if (!ptr)
		return;

	/*
	 * Ensure "resilience" is a standalone token, not a substring
	 * of another word (e.g., "no_resilience" or "resilience_other").
	 * Check both prefix and suffix boundaries.
	 */
	if (ptr != job_desc->comment) {
		char prev = *(ptr - 1);
		if (prev != ' ' && prev != ',' && prev != ';')
			return;
	}
	ptr += strlen("resilience");
	if (*ptr != '\0' && *ptr != ' ' && *ptr != ',' && *ptr != '=')
		return;

	/* Parse optional threshold: resilience=80 */
	if (*ptr == '=') {
		pct = (int) strtol(ptr + 1, NULL, 10);
		if (pct < 1 || pct > 100)
			pct = 0;
	}

	job_desc->bitflags |= ADAPTIVE_RESILIENCE;

	/*
	 * Encode the custom threshold in admin_comment so that
	 * the slurmctld can extract it when building the job record.
	 * This avoids extending job_desc_msg_t and the wire protocol.
	 *
	 * Append rather than overwrite to preserve any existing
	 * admin_comment content set by other plugins or the admin.
	 */
	if (pct > 0) {
		if (job_desc->admin_comment &&
		    job_desc->admin_comment[0] != '\0')
			xstrfmtcat(job_desc->admin_comment,
				   " resilience_pct=%d", pct);
		else
			xstrfmtcat(job_desc->admin_comment,
				   "resilience_pct=%d", pct);
	}
}

extern int job_submit(job_desc_msg_t *job_desc, uint32_t submit_uid,
		      char **err_msg)
{
	_parse_resilience_comment(job_desc);

	/* Admin partition-level opt-in */
	if (!(job_desc->bitflags & ADAPTIVE_RESILIENCE) &&
	    _partition_has_resilience(job_desc->partition)) {
		job_desc->bitflags |= ADAPTIVE_RESILIENCE;
	}

	/*
	 * Resilience requires kill_on_node_fail=0 to allow the job to
	 * survive node loss. Ensure this is set when resilience is active.
	 */
	if (job_desc->bitflags & ADAPTIVE_RESILIENCE) {
		job_desc->kill_on_node_fail = 0;
		info("job_submit/resilience: enabled adaptive resilience for uid=%u",
		     submit_uid);
	}

	return SLURM_SUCCESS;
}

extern int job_modify(job_desc_msg_t *job_desc, job_record_t *job_ptr,
		      uint32_t submit_uid, char **err_msg)
{
	_parse_resilience_comment(job_desc);

	/* Admin partition-level opt-in */
	if (!(job_desc->bitflags & ADAPTIVE_RESILIENCE) &&
	    job_ptr->part_ptr &&
	    _partition_has_resilience(job_ptr->part_ptr->name)) {
		job_desc->bitflags |= ADAPTIVE_RESILIENCE;
	}

	/*
	 * Ensure kill_on_node_fail is cleared when resilience is
	 * enabled via job modification, matching the job_submit path.
	 */
	if (job_desc->bitflags & ADAPTIVE_RESILIENCE) {
		job_desc->kill_on_node_fail = 0;
		info("job_submit/resilience: enabled adaptive resilience for modified job %u uid=%u",
		     job_ptr->job_id, submit_uid);
	}

	return SLURM_SUCCESS;
}
