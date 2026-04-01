/*
 * fb_summary_service.h
 *    Background summary prebuild service.
 */

#ifndef FB_SUMMARY_SERVICE_H
#define FB_SUMMARY_SERVICE_H

#include "postgres.h"

void fb_summary_service_shmem_init(void);
void fb_summary_service_report_query_summary_usage(TimestampTz observed_at,
												   uint32 summary_span_fallback_segments,
												   uint32 metadata_fallback_segments);

#endif
