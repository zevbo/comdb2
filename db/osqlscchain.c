#include "osqlscchain.h"
#include <schemachange.h>
#include "logmsg.h"
#include <sc_chain.h>
#include <math.h>
#include <sc_queues.h>
#include <mem_override.h>

// Gets the table name from the trigger csc2
static char *get_trigger_table_name(char *newcsc2){
    newcsc2 += strlen("table ");
    int len = 0;
    while(newcsc2[len] != '\n'){len++;}
    char *table_name = malloc((len + 1) * sizeof(char));
    strncpy(table_name, newcsc2, len);
    table_name[len] = '\0';
    return table_name;
}

static struct schema_change_type *gen_audited_lua(char *table_name, char *spname){
	char *code_start = 
    "local function main(event)\n"
    "	local audit = db:table('";
    char * code_end = 
        "')\n"
    "    local chg = {}\n"
    "    if event.new ~= nil then\n"
    "        for k, v in pairs(event.new) do\n"
    "            chg['new_'..k] = v\n"
    "        end\n"
    "    end\n"
    "    if event.old ~= nil then\n"
    "        for k, v in pairs(event.old) do\n"
    "            chg['old_'..k] = v\n"
    "        end\n"
    "    end\n"
    "    chg.type = event.type\n"
    "    chg.tbl = event.name\n"
    "    chg.logtime = db:now()\n"
    "    return audit:insert(chg)\n"
    "end\n";
    char *code = malloc((strlen(code_start) + strlen(code_end) + strlen(table_name) + 1) * sizeof(char));
    strcpy(code, code_start);
    strcat(code, table_name);
    strcat(code, code_end);

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);
    sc->addsp = 1;
    sc->newcsc2 = code;
    strcpy(sc->fname, "built-in audit");
	return sc;
}

static struct schema_change_type *populate_audited_trigger_chain(struct schema_change_type *sc){
    char *tablename = get_trigger_table_name(sc->newcsc2);
    struct schema_change_type *sc_full = create_audit_table_sc(tablename);
    struct schema_change_type *sc_proc = gen_audited_lua(sc_full->tablename, sc->tablename + 3);
    append_to_chain(sc_full, sc_proc);
    append_to_chain(sc_full, sc);
    sc->dont_expand = 0;
    sc->audit_table = sc_full->tablename;
    sc->trigger_table = tablename;
    return sc_full;
}

extern int gbl_cary_alters_to_audits;

// zTODOc: Better name
static struct schema_change_type *make_audit_alters_nothrevent(struct schema_change_type *sc){
    
    char **audits;
    int num_audits;

    bdb_get_audited_sp_tran(sc->tran, sc->tablename, &audits, &num_audits, TABLE_TO_AUDITS);
    if(num_audits > 0){sc->nothrevent = 1;}

    return sc;

}

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc){
    if (sc->is_trigger == AUDITED_TRIGGER && sc->dont_expand){
        return populate_audited_trigger_chain(sc);
    } else if (sc->alteronly && sc->newcsc2 && gbl_cary_alters_to_audits) {
        return make_audit_alters_nothrevent(sc);
    } else {
        return sc;
    }
}