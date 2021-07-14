#ifndef _OSQL_CHAIN_H_
#define _OSQL_CHAIN_H_

#include "comdb2.h"
#include "sbuf2.h"
#include "osqlsession.h"
#include "sqloffload.h"
#include "block_internal.h"
#include "comdb2uuid.h"

/* The idea is here you can do an initial populate. However, you can 
 also add schema changes to the queues while they are being prepared 
 in the schema change folder. This is critical for dynamically choosing
 schema changes based on results of previous schema changes. I think
 we still need to keep this initial populate though, as sometimes you
 need to flip schema changes so that the original schema change isn't executed
 first. Also, it is nice to be able to do stuff based on the first sc.
 With that said (zTODO?) it might be beneficial to get rid of this one
 as it can be replaced with schema changes that do not do anything except
 for populating more schema changes in the chain */

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc);
#endif