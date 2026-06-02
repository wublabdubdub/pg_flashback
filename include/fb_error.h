/*
 * fb_error.h
 *    Shared error reporting helpers.
 */

#ifndef FB_ERROR_H
#define FB_ERROR_H

#include "postgres.h"

/*
 * fb_raise_not_implemented
 *    Error API.
 */

void fb_raise_not_implemented(const char *feature_name);
/*
 * fb_raise_unsupported_relation
 *    Error API.
 */

void fb_raise_unsupported_relation(const char *reason);

#endif
