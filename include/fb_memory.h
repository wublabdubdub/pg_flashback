#ifndef FB_MEMORY_H
#define FB_MEMORY_H

#include "postgres.h"

#include "access/htup_details.h"

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
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("pg_flashback memory limit exceeded while tracking %s", what),
				 errdetail("tracked=%llu bytes limit=%llu bytes requested=%zu bytes",
						   (unsigned long long) *tracked_bytes,
						   (unsigned long long) limit_bytes,
						   bytes)));

	*tracked_bytes += (uint64) bytes;
}

static inline Size
fb_memory_cstring_bytes(const char *value)
{
	if (value == NULL)
		return 0;

	return strlen(value) + 1;
}

static inline Size
fb_memory_heaptuple_bytes(HeapTuple tuple)
{
	if (tuple == NULL)
		return 0;

	return HEAPTUPLESIZE + tuple->t_len;
}

#endif
