#include "sc_chain.h"
#include "schemachange.h"
#include "logmsg.h"
#include <math.h>

void append_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next){
    if (sc->sc_chain_next){
        append_to_chain(sc->sc_chain_next, sc_chain_next);
    } else {
        sc->sc_chain_next = sc_chain_next;
    }
}

void add_next_to_chain(struct schema_change_type *sc, struct schema_change_type *sc_chain_next){
    if (sc_chain_next->sc_chain_next){
        add_next_to_chain(sc, sc_chain_next->sc_chain_next);
    }
    sc_chain_next->sc_chain_next = sc->sc_chain_next;
    sc->sc_chain_next = sc_chain_next;
}