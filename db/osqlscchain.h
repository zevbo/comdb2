#ifndef _OSQL_CHAIN_H_
#define _OSQL_CHAIN_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"

/* Note: here we are limited by the fact that we must decide what and how many schema changes we
are going to do before any of the earlier schema changes have occured */

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc);
#endif