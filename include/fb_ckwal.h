/*
 * fb_ckwal.h
 *    Embedded ckwal recovery interfaces for missing or mismatched WAL.
 */

#ifndef FB_CKWAL_H
#define FB_CKWAL_H

#include "access/xlogdefs.h"
#include "postgres.h"

/*
 * fb_ckwal_restore_segment
 *    ckwal API.
 */

bool fb_ckwal_restore_segment(TimeLineID timeline_id,
							  XLogSegNo segno,
							  int wal_seg_size,
							  char *out_path,
							  Size out_path_len);
/*
 * fb_ckwal_convert_mismatched_segment
 *    ckwal API.
 */

bool fb_ckwal_convert_mismatched_segment(const char *src_path,
										 int wal_seg_size,
										 TimeLineID *timeline_id,
										 XLogSegNo *segno,
										 char *out_path,
										 Size out_path_len);

#endif
