#include "osqlscchain.h"
#include <schemachange.h>
#include "logmsg.h"
#include <sc_chain.h>
#include <math.h>
#include <sc_queues.h>

// Errors I have gotten here:
// 1. Two schema changes with the same tablename; ie: don't use reserved resources

static struct schema_change_type *create_audit_table_sc(char *name){
	struct schema_change_type *sc = new_schemachange_type();
    sc->sc_chain_next = NULL;
	// Guesses
    sc->onstack = 1;
	sc->type = DBTYPE_TAGGED_TABLE;
	sc->scanmode = gbl_default_sc_scanmode;
    sc->live = 1;
	// Maybe need use_plan?
	sc->addonly = 1;

	char *prefix = "$audit_";
	strcpy(sc->tablename, prefix);
	strcat(sc->tablename, name);
    // zTODO: I think that get_dbtable_by_name ultimately frees name. If it doesn't we have a problem: some undefined behavior somewhere
    // To see odd behavior, simply look at the contents of name after get_audit_schema is called
    // It should be something like "es }" which is the ending to the audit schema in get_audit_schema
    // Update: No longer sure the above statment is correct. Might be fine now
	struct dbtable *db = get_dbtable_by_name(name);
	sc->newcsc2 = get_audit_schema(db->schema);

    if (db->instant_schema_change) sc->instant_sc = 1;

	// What is ODH? This is just copied from timepart
	if (db->odh) sc->headers = 1;

    int postfix_start = strlen(sc->tablename);
    for(int i = 2; get_dbtable_by_name(sc->tablename); i++){
        char *postfix_str = malloc((ceil(log(i)) + 1) * sizeof(char));
        sprintf(postfix_str, "$%d", i);
        strcpy(sc->tablename + postfix_start, postfix_str);
        free(postfix_str);
    }

	return sc;
}

// zTODO: make sure that this format won't change up on us
static char *get_trigger_table_name(char *newcsc2){
    newcsc2 += strlen("table ");
    int len = 0;
    while(newcsc2[len] != '\n'){len++;}
    // zTODO: fix all mallocs to some sort of comdb2 malloc
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
    /*
	zTODO: Got to make this work at some point
    if (comdb2TokenToStr(nm, spname, sizeof(spname))) {
        setError(pParse, SQLITE_MISUSE, "Procedure name is too long");
        logmsg(LOGMSG_WARN, "Failure on comdb2TokenToStr\n");
        return;
    }
    */

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);
    sc->addsp = 1;
    sc->newcsc2 = code;
    // zTODO: sc_chain_next should automatically be NULL
    strcpy(sc->fname, "built-in audit");
	return sc;
}

static struct schema_change_type *populate_audited_trigger_chain(struct schema_change_type *sc){
    char *tablename = get_trigger_table_name(sc->newcsc2);
    struct schema_change_type *sc_full = create_audit_table_sc(tablename);
    struct schema_change_type *sc_proc = gen_audited_lua(sc_full->tablename, sc->tablename + 3);
    append_to_chain(sc_full, sc_proc);
    append_to_chain(sc_full, sc);
    sc->audit_table = sc_full->tablename;
    sc->trigger_table = tablename;
    return sc_full;
}
// zTODO: Better name
void fix_alter(struct schema_change_type *sc){
    
    char **audits;
    int num_audits;
    // zTODO: Is version important here?
    // zTODO: is the dbenv correct?
    bdb_get_audited_sp_tran(sc->tran, sc->tablename, &audits, &num_audits);
    if (num_audits > 0){
        sc->nothrevent = 1;
    }
}

struct schema_change_type *populate_sc_chain(struct schema_change_type *sc){
    // zTODO: I'm putting this here cause I already want to kms b/c of it and this will remind me how bad it is
    sc->create_version_schema = create_version_schema;
    fix_alter(sc);
    if (sc->is_trigger == AUDITED_TRIGGER){
        return populate_audited_trigger_chain(sc);
    } else {
        return sc;
    }
}