#ifndef FB_CKWAL_H
#define FB_CKWAL_H

#include "access/xlogdefs.h"
#include "postgres.h"

bool fb_ckwal_restore_segment(TimeLineID timeline_id,
							  XLogSegNo segno,
							  int wal_seg_size,
							  char *out_path,
							  Size out_path_len);
bool fb_ckwal_convert_mismatched_segment(const char *src_path,
										 int wal_seg_size,
										 TimeLineID *timeline_id,
										 XLogSegNo *segno,
										 char *out_path,
										 Size out_path_len);

#endif
