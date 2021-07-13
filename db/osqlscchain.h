#ifndef _OSQL_CHAIN_H_
#define _OSQL_CHAIN_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"

struct schema_change_type * populate_sc_chain(struct schema_change_type *sc);

#endif