/*
 * fb_memory.h
 *    Inline helpers for query memory accounting.
 */

#ifndef FB_MEMORY_H
#define FB_MEMORY_H

#include "postgres.h"

#include "access/htup_details.h"
#include "lib/stringinfo.h"

/*
 * fb_memory_append_bytes_value
 *    Memory accounting inline helper.
 */

static inline void
fb_memory_append_bytes_value(StringInfo buf, uint64 bytes)
{
	static const char *const units[] = {"bytes", "KB", "MB", "GB", "TB", "PB", "EB"};
	uint64 promoted = bytes;
	int unit_index = 0;

	appendStringInfo(buf, "%llu bytes", (unsigned long long) bytes);

	while (promoted > 0 &&
		   unit_index < (lengthof(units) - 1) &&
		   (promoted % 1024) == 0)
	{
		promoted /= 1024;
		unit_index++;
	}

	if (unit_index > 0)
		appendStringInfo(buf, " (%llu %s)",
						 (unsigned long long) promoted,
						 units[unit_index]);
}

/*
 * fb_memory_charge_bytes
 *    Memory accounting inline helper.
 */

static inline void
fb_memory_charge_bytes(uint64 *tracked_bytes,
					   uint64 limit_bytes,
					   Size bytes,
					   const char *what)
{
	if (tracked_bytes == NULL || bytes == 0)
		return;

	if (limit_bytes > 0 &&
		*tracked_bytes + (uint64) bytes > limit_bytes)
	{
		StringInfoData detail;

		initStringInfo(&detail);
		appendStringInfoString(&detail, "tracked=");
		fb_memory_append_bytes_value(&detail, *tracked_bytes);
		appendStringInfoString(&detail, " limit=");
		fb_memory_append_bytes_value(&detail, limit_bytes);
		appendStringInfoString(&detail, " requested=");
		fb_memory_append_bytes_value(&detail, bytes);

		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("pg_flashback memory limit exceeded while tracking %s", what),
				 errdetail_internal("%s", detail.data),
				 errhint("Consider increasing pg_flashback.memory_limit for this query.")));
	}

	*tracked_bytes += (uint64) bytes;
}

/*
 * fb_memory_release_bytes
 *    Memory accounting inline helper.
 */

static inline void
fb_memory_release_bytes(uint64 *tracked_bytes, Size bytes)
{
	if (tracked_bytes == NULL || bytes == 0)
		return;

	if (*tracked_bytes <= (uint64) bytes)
	{
		*tracked_bytes = 0;
		return;
	}

	*tracked_bytes -= (uint64) bytes;
}

/*
 * fb_memory_cstring_bytes
 *    Memory accounting inline helper.
 */

static inline Size
fb_memory_cstring_bytes(const char *value)
{
	if (value == NULL)
		return 0;

	return strlen(value) + 1;
}

/*
 * fb_memory_heaptuple_bytes
 *    Memory accounting inline helper.
 */

static inline Size
fb_memory_heaptuple_bytes(HeapTuple tuple)
{
	if (tuple == NULL)
		return 0;

	return HEAPTUPLESIZE + tuple->t_len;
}

#endif
