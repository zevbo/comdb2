#ifndef _SC_CHAIN_H_
#define _SC_CHAIN_H_

#include "schemachange.h"

void append_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next);
void add_next_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next);
#endif