#include "postgres.h"

#include "utils/elog.h"

#include "fb_error.h"

void
fb_raise_not_implemented(const char *feature_name)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s is not implemented yet", feature_name)));
}

void
fb_raise_unsupported_relation(const char *reason)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("fb does not support %s", reason)));
}
