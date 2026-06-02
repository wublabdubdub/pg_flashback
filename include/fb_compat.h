/*
 * fb_compat.h
 *    Compatibility shims for supported PostgreSQL versions.
 */

#ifndef FB_COMPAT_H
#define FB_COMPAT_H

#include "miscadmin.h"
#include "access/genam.h"
#include "access/xlogreader.h"
#include "storage/fd.h"
#include "storage/shm_mq.h"

#if PG_VERSION_NUM >= 160000
#include "storage/relfilelocator.h"
#else
#include "storage/relfilenode.h"
typedef RelFileNode RelFileLocator;
#ifndef RelFileLocatorEquals
#define RelFileLocatorEquals(locator1, locator2) \
	RelFileNodeEquals((locator1), (locator2))
#endif
#endif

#if PG_VERSION_NUM >= 160000
#define FB_RELATION_LOCATOR(rel) ((rel)->rd_locator)
#define FB_LOCATOR_DBOID(locator) ((locator).dbOid)
#define FB_LOCATOR_SPCOID(locator) ((locator).spcOid)
#define FB_LOCATOR_RELNUMBER(locator) ((locator).relNumber)
#else
#define FB_RELATION_LOCATOR(rel) ((rel)->rd_node)
#define FB_LOCATOR_DBOID(locator) ((locator).dbNode)
#define FB_LOCATOR_SPCOID(locator) ((locator).spcNode)
#define FB_LOCATOR_RELNUMBER(locator) ((locator).relNode)
#endif

#if PG_VERSION_NUM >= 110000
#define FB_INDEX_NKEYATTS(index_form) ((index_form)->indnkeyatts)
#else
#define FB_INDEX_NKEYATTS(index_form) ((index_form)->indnatts)
#endif

#if PG_VERSION_NUM >= 160000
#define FB_XL_SMGR_CREATE_LOCATOR(xlrec) ((xlrec)->rlocator)
#define FB_XL_SMGR_TRUNCATE_LOCATOR(xlrec) ((xlrec)->rlocator)
#else
#define FB_XL_SMGR_CREATE_LOCATOR(xlrec) ((xlrec)->rnode)
#define FB_XL_SMGR_TRUNCATE_LOCATOR(xlrec) ((xlrec)->rnode)
#endif

static inline bool
fb_xlogrec_get_block_tag(XLogReaderState *reader,
						 uint8 block_id,
						 RelFileLocator *locator,
						 ForkNumber *forknum,
						 BlockNumber *blkno)
{
#if PG_VERSION_NUM >= 150000
	return XLogRecGetBlockTagExtended(reader, block_id, locator, forknum,
									  blkno, NULL);
#else
	return XLogRecGetBlockTag(reader, block_id, locator, forknum, blkno);
#endif
}

static inline ssize_t
fb_file_read_compat(File file,
					void *buffer,
					size_t amount,
					off_t offset,
					uint32 wait_event_info)
{
#if PG_VERSION_NUM >= 180000
	struct iovec iov = {
		.iov_base = buffer,
		.iov_len = amount
	};

	return FileReadV(file, &iov, 1, offset, wait_event_info);
#else
	return FileRead(file, buffer, amount, offset, wait_event_info);
#endif
}

static inline ssize_t
fb_file_write_compat(File file,
					 const void *buffer,
					 size_t amount,
					 off_t offset,
					 uint32 wait_event_info)
{
#if PG_VERSION_NUM >= 180000
	struct iovec iov = {
		.iov_base = unconstify(void *, buffer),
		.iov_len = amount
	};

	return FileWriteV(file, &iov, 1, offset, wait_event_info);
#else
	return FileWrite(file, buffer, amount, offset, wait_event_info);
#endif
}

#if PG_VERSION_NUM >= 160000
#define FB_XLOGREC_HAS_BLOCK_DATA(reader, block_id) \
	XLogRecHasBlockData((reader), (block_id))
#elif PG_VERSION_NUM >= 150000
#define FB_XLOGREC_HAS_BLOCK_DATA(reader, block_id) \
	(XLogRecGetBlock((reader), (block_id))->has_data)
#else
#define FB_XLOGREC_HAS_BLOCK_DATA(reader, block_id) \
	((reader)->blocks[(block_id)].has_data)
#endif

#if PG_VERSION_NUM >= 150000
#define FB_XLOGREC_MAX_BLOCK_ID(reader) XLogRecMaxBlockId(reader)
#else
#define FB_XLOGREC_MAX_BLOCK_ID(reader) ((reader)->max_block_id)
#endif

#if PG_VERSION_NUM >= 140000
#define FB_XLOGREC_GET_TOP_XID(reader) XLogRecGetTopXid(reader)
#else
#define FB_XLOGREC_GET_TOP_XID(reader) XLogRecGetXid(reader)
#endif

#if PG_VERSION_NUM >= 160000
#define FB_XL_HEAP_LOCK_XMAX(xlrec) ((xlrec)->xmax)
#else
#define FB_XL_HEAP_LOCK_XMAX(xlrec) ((xlrec)->locking_xid)
#endif

#if PG_VERSION_NUM >= 140000
#define FB_XLH_INSERT_ALL_FROZEN_SET XLH_INSERT_ALL_FROZEN_SET
#else
#define FB_XLH_INSERT_ALL_FROZEN_SET 0
#endif

void fb_mark_guc_prefix_reserved(const char *class_name);
void *fb_guc_malloc_compat(size_t size);
char *fb_guc_strdup_compat(const char *src);
void fb_guc_free_compat(void *ptr);
int fb_mkdir_p_compat(const char *path, int omode);
XLogRecPtr fb_xlog_find_next_record_compat(XLogReaderState *state,
										   XLogRecPtr recptr);
IndexScanDesc fb_index_beginscan_compat(Relation heap_relation,
										Relation index_relation,
										Snapshot snapshot,
										int nkeys,
										int norderbys);
shm_mq_result fb_shm_mq_send_compat(shm_mq_handle *mqh,
									Size nbytes,
									const void *data,
									bool nowait);

#endif
