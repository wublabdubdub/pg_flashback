/*
 * fb_compat.c
 *    PostgreSQL version compatibility translation unit.
 */

#include "postgres.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "access/xlog_internal.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "fb_compat.h"

void
fb_mark_guc_prefix_reserved(const char *class_name)
{
#if PG_VERSION_NUM >= 150000
	MarkGUCPrefixReserved(class_name);
#else
	EmitWarningsOnPlaceholders(class_name);
#endif
}

void *
fb_guc_malloc_compat(size_t size)
{
#if PG_VERSION_NUM >= 180000
	return guc_malloc(ERROR, size);
#else
	void *ptr;

	if (size == 0)
		size = 1;

	ptr = malloc(size);
	if (ptr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	return ptr;
#endif
}

char *
fb_guc_strdup_compat(const char *src)
{
#if PG_VERSION_NUM >= 180000
	return guc_strdup(ERROR, src);
#else
	char *dst;
	size_t len;

	if (src == NULL)
		return NULL;

	len = strlen(src) + 1;
	dst = fb_guc_malloc_compat(len);
	memcpy(dst, src, len);
	return dst;
#endif
}

void
fb_guc_free_compat(void *ptr)
{
#if PG_VERSION_NUM >= 150000
#if PG_VERSION_NUM >= 180000
	guc_free(ptr);
#else
	free(ptr);
#endif
#else
	free(ptr);
#endif
}

int
fb_mkdir_p_compat(const char *path, int omode)
{
#if PG_VERSION_NUM >= 180000
	char *mutable_path;
	int rc;

	mutable_path = pstrdup(path);
	rc = pg_mkdir_p(mutable_path, omode);
	pfree(mutable_path);
	return rc;
#else
	char *mutable_path;
	char *slash;
	int rc = 0;

	(void) omode;

	if (path == NULL || path[0] == '\0')
		return -1;

	mutable_path = pstrdup(path);
	for (slash = mutable_path + 1; *slash != '\0'; slash++)
	{
		if (*slash != '/')
			continue;

		*slash = '\0';
		if (MakePGDirectory(mutable_path) < 0 && errno != EEXIST)
		{
			rc = -1;
			*slash = '/';
			break;
		}
		*slash = '/';
	}

	if (rc == 0 &&
		MakePGDirectory(mutable_path) < 0 &&
		errno != EEXIST)
		rc = -1;

	pfree(mutable_path);
	return rc;
#endif
}

XLogRecPtr
fb_xlog_find_next_record_compat(XLogReaderState *state, XLogRecPtr recptr)
{
#if PG_VERSION_NUM >= 180000
	return XLogFindNextRecord(state, recptr);
#else
	XLogRecPtr	pageptr;
	uint32		startoff;
	uint32		page_header_size;
	uint32		contlen = 0;
	int			readlen;
	char	   *errormsg = NULL;

	if (state == NULL || XLogRecPtrIsInvalid(recptr))
		return InvalidXLogRecPtr;

	pageptr = recptr - (recptr % XLOG_BLCKSZ);

	for (;;)
	{
		XLogPageHeader page_header;

		readlen = state->routine.page_read(state,
										   pageptr,
										   SizeOfXLogShortPHD,
										   recptr,
										   state->readBuf);
		if (readlen < 0)
			return InvalidXLogRecPtr;

		page_header = (XLogPageHeader) state->readBuf;
		page_header_size = XLogPageHeaderSize(page_header);
		if (readlen < (int) page_header_size)
		{
			readlen = state->routine.page_read(state,
											   pageptr,
											   page_header_size,
											   recptr,
											   state->readBuf);
			if (readlen < (int) page_header_size)
				return InvalidXLogRecPtr;
			page_header = (XLogPageHeader) state->readBuf;
		}

		startoff = (pageptr == recptr - (recptr % XLOG_BLCKSZ)) ?
			(uint32) (recptr % XLOG_BLCKSZ) : 0;
		if (startoff < page_header_size)
			startoff = page_header_size;

		if ((page_header->xlp_info & XLP_FIRST_IS_CONTRECORD) != 0 &&
			startoff == page_header_size)
		{
			contlen = Min((uint32) page_header->xlp_rem_len,
						  (uint32) (XLOG_BLCKSZ - page_header_size));
			startoff += contlen;
		}

		for (; startoff < XLOG_BLCKSZ; startoff++)
		{
			XLogBeginRead(state, pageptr + startoff);
			if (XLogReadRecord(state, &errormsg) != NULL)
				return state->ReadRecPtr;
		}

		pageptr += XLOG_BLCKSZ;
		recptr = pageptr;
	}
#endif
}

IndexScanDesc
fb_index_beginscan_compat(Relation heap_relation,
						  Relation index_relation,
						  Snapshot snapshot,
						  int nkeys,
						  int norderbys)
{
#if PG_VERSION_NUM >= 180000
	return index_beginscan(heap_relation,
						   index_relation,
						   snapshot,
						   NULL,
						   nkeys,
						   norderbys);
#else
	return index_beginscan(heap_relation,
						   index_relation,
						   snapshot,
						   nkeys,
						   norderbys);
#endif
}

shm_mq_result
fb_shm_mq_send_compat(shm_mq_handle *mqh,
					  Size nbytes,
					  const void *data,
					  bool nowait)
{
#if PG_VERSION_NUM >= 150000
	return shm_mq_send(mqh, nbytes, data, nowait, false);
#else
	return shm_mq_send(mqh, nbytes, data, nowait);
#endif
}
