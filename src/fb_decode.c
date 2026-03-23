#include "postgres.h"

#include "fb_decode.h"

/*
 * Replay-backed row-image debug entry points live in fb_entry.c so they can
 * share the normal runtime gate chain. Decode-specific helpers will return
 * here as the P4 implementation grows.
 */
