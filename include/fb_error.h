#ifndef FB_ERROR_H
#define FB_ERROR_H

#include "postgres.h"

void fb_raise_not_implemented(const char *feature_name);
void fb_raise_unsupported_relation(const char *reason);

#endif
