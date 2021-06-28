#include <string.h>
#include <poll.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <berkdb/dbinc/queue.h>
#include <schemachange.h>
#include <sc_struct.h>
#include <strbuf.h>
#include <sqliteInt.h>
#include <comdb2build.h>
#include <comdb2vdbe.h>
#include <trigger.h>
#include <sqlglue.h>

struct dbtable;
struct dbtable *getqueuebyname(const char *);
int bdb_get_sp_get_default_version(const char *, int *);
#define COMDB2_NOT_AUTHORIZED_ERRMSG "comdb2: not authorized"

int comdb2LocateSP(Parse *p, char *sp)
{
	char *ver = NULL;
	int bdberr;
	int rc0 = bdb_get_sp_get_default_version(sp, &bdberr);
	int rc1 = bdb_get_default_versioned_sp(sp, &ver);
	free(ver);
	if (rc0 < 0 && rc1 < 0) {
		sqlite3ErrorMsg(p, "no such procedure: %s", sp);
		return -1;
	}
	return 0;
}

enum ops { del = 0x01, ins = 0x02, upd = 0x04 };

typedef struct columnevent {
	const char *col;
	int event;
	LIST_ENTRY(columnevent) link;
} ColumnEvent;

typedef struct {
	LIST_HEAD(, columnevent) head;
} ColumnEventList;

static ColumnEvent *getcol(ColumnEventList *list, const char *col)
{
	ColumnEvent *e = NULL;
	LIST_FOREACH(e, &list->head, link) {
		if (strcmp(e->col, col) == 0) {
			return e;
		}
	}
	e = malloc(sizeof(ColumnEvent));
	e->col = col;
	e->event = 0;
	LIST_INSERT_HEAD(&list->head, e, link);
	return e;
}

#define ALLOW_ALL_COLS
static void add_watched_cols(int type, Table *table, Cdb2TrigEvent *event,
			     ColumnEventList *list)
{
	if (event->cols) {
		for (int i = 0; i < event->cols->nId; ++i) {
			ColumnEvent *ce = getcol(list, event->cols->a[i].zName);
			ce->event |= type;
		}
	#ifdef ALLOW_ALL_COLS
	} else {
		for (int i = 0; i < table->nCol; ++i) {
			ColumnEvent *ce = getcol(list, table->aCol[i].zName);
			ce->event |= type;
		}
	#endif
	}
}

Cdb2TrigEvents *comdb2AddTriggerEvent(Parse *pParse, Cdb2TrigEvents *A, Cdb2TrigEvent *B)
{
	if (A == NULL) {
		A = sqlite3DbMallocZero(pParse->db, sizeof(Cdb2TrigEvents));
	}
	Cdb2TrigEvent *e = NULL;
	const char *type = NULL;
	switch (B->op) {
	case TK_DELETE: e = &A->del; type = "delete"; break;
	case TK_INSERT: e = &A->ins; type = "insert"; break;
	case TK_UPDATE: e = &A->upd; type = "update"; break;
	default: sqlite3ErrorMsg(pParse, "%s: bad op", __func__, B->op); return NULL;
	}
	if (B->op == e->op) {
		sqlite3DbFree(pParse->db, A);
		sqlite3ErrorMsg(pParse, "%s condition repeated", type);
		return NULL;
	#ifndef ALLOW_ALL_COLS
	} else if (B->cols == NULL) {
		sqlite3DbFree(pParse->db, A);
		sqlite3ErrorMsg(pParse, "%s condition has unspecified columns", type);
		return NULL;
	#endif
	}
	e->op = B->op;
	e->cols = B->cols;
	return A;
}

Cdb2TrigTables *comdb2AddTriggerTable(Parse *parse, Cdb2TrigTables *tables,
				      SrcList *tbl, Cdb2TrigEvents *events)
{
	Table *table;
	if ((table = sqlite3LocateTableItem(parse, 0, &tbl->a[0])) == NULL) {
		sqlite3ErrorMsg(parse, "no such table:%s", tbl->a[0].zName);
		return NULL;
	}
	Cdb2TrigTables *tmp;
	const char *name = table->zName;
	if (tables) {
		tmp = tables;
		while (tmp) {
			if (strcmp(tmp->table->zName, name) == 0) {
				sqlite3ErrorMsg(parse, "trigger already specified table:%s", name);
				return NULL;
			}
			tmp = tmp->next;
		}
	}
	tmp = sqlite3DbMallocRaw(parse->db, sizeof(Cdb2TrigTables));
	if (tmp == NULL) {
		sqlite3ErrorMsg(parse, "malloc failED");
		return NULL;
	}
	tmp->table = table;
	tmp->events = events;
	tmp->next = tables;
	return tmp;
}
int isSchemaWhitespace(char c){
	/* The ']' is here because that effectively acts as a whitespace seperator
	   So does '[', but for these purposes that is unnecessary */
	return c == ' ' || c == '\n' || c == ']';
}
char **get_entries(dbtable *db, int nCol){
	char *old_csc2 = NULL;
	if (get_csc2_file(db->tablename, -1 /*highest csc2_version*/, &old_csc2,
                      NULL /*csc2len*/)) {
        logmsg(LOGMSG_ERROR, "could not get schema (audited trigger)\n");
        return NULL;
    }
	// TODO: this assumes that the schema is defined first
	while(*old_csc2 != '{'){
		old_csc2++;
	}
	old_csc2++;
	int index_on = 0;
	/* TODO: malloc -> comdb2_malloc */
	char **entries = malloc(nCol * sizeof(char *));
	for(int entry_on = 0; old_csc2[entry_on] != '}'; entry_on++){
		/* ws = whitespace */
		int search_index = index_on;
		/* on_whitespace starts as true so that we can effectively 
		trim the start of the string */
		int on_whitespace = 1;
		/* ws_found starts as -1 because when we finish trimming
		the string, it will increase ws_found by 1*/
		for(int ws_found = 0; ws_found < 2; search_index++){
			if (old_csc2[search_index] == '}'){
				break;
			}
			int now_on_ws = isSchemaWhitespace(old_csc2[search_index]);
			if (!on_whitespace && now_on_ws) {
				ws_found++;
			}
			on_whitespace = now_on_ws;
		}
		if (old_csc2[search_index] == '}'){
			break;
		}
		while(isSchemaWhitespace(old_csc2[search_index])){search_index++;}
		/* deals with arrays such as "cstring text [100] */
		if (old_csc2[search_index] == '[') {
			while(old_csc2[search_index] != ']'){search_index++;}
			while(isSchemaWhitespace(old_csc2[search_index])){search_index++;}
		}
		int entry_len = search_index - index_on;
		logmsg(LOGMSG_WARN, "entry len: %d\n", entry_len);
		char *entry = malloc((entry_len + 1) * sizeof(char));
		for(int i = 0; i < entry_len; i++){
			entry[i] = old_csc2[index_on + i];
		}
		entry[entry_len] = 0;
		index_on = search_index;
		entries[entry_on] = entry;

		while(1){
			int i;
			for(i = index_on; 
				!isSchemaWhitespace(old_csc2[i]) && old_csc2[i] != '='; i++){
				// Skip to next word (plausibly an equals)
			}
			while(isSchemaWhitespace(old_csc2[i])){i++;}
			if(old_csc2[i] == '='){
				i++;
				// Skip to next word
				while(isSchemaWhitespace(old_csc2[i])){i++;}
				// Skip to next skip word
				while(old_csc2[i] != '}' && !isSchemaWhitespace(old_csc2[i])){i++;}
				// Skip to next line or attr
				while(isSchemaWhitespace(old_csc2[i])){i++;}
				index_on = i;
			} else {
				break;
			}
		} 
	}
	return entries;
} 
char *get_audit_schema(dbtable *db, int nCol){
	char **entries = get_entries(db, nCol);
	int len = 0;
	char *schema_start = "schema {cstring type[4] cstring tbl[64] datetime logtime ";
	len += strlen(schema_start);
	/* "}" */
	len += 1;
	char *line_postfix = "null=yes ";
	for(int i = 0; i < nCol; i++){
		int line_size = strlen(entries[i]) + strlen(line_postfix);
		int new_line = line_size + 5; /* +1 is for the "new_ " */
		int old_line = line_size + 5; /* +5 is for the "old_ " */
		len += new_line + old_line;
	}
	char *audit_schema = malloc((len + 1) * sizeof(char));
	strcpy(audit_schema, schema_start);
	int len_on = strlen(schema_start);
	for(int i = 0; i < nCol; i++){
		char *entry = entries[i];
		int name_index = 0;
		int on_whitespace = 1;
		for(int ws_found = -1; ws_found < 1; name_index++){
			int now_on_ws = isSchemaWhitespace(entry[name_index]);
			if (on_whitespace && !now_on_ws) {
				ws_found++;
			}
			on_whitespace = now_on_ws;
		}
		/* Name index decremneted so that it doesn't cut off first char of name */
		name_index--;
		char *type = malloc((name_index + 1) * sizeof(char));
		char *name = malloc((strlen(entry) - name_index + 1) * sizeof(char));
		memcpy(type, entry, name_index);
		type[name_index + 1] = '\0';
		strcpy(name, entry + name_index);

		strcpy(audit_schema + len_on, type);
		len_on += strlen(type);
		strcpy(audit_schema + len_on, "new_");
		len_on += 4;
		strcpy(audit_schema + len_on, name);
		len_on += strlen(name);
		strcpy(audit_schema + len_on, " ");
		len_on += 1;
		strcpy(audit_schema + len_on, line_postfix);
		len_on += strlen(line_postfix);

		strcpy(audit_schema + len_on, type);
		len_on += strlen(type);
		strcpy(audit_schema + len_on, "old_");
		len_on += 4;
		strcpy(audit_schema + len_on, name);
		len_on += strlen(name);
		strcpy(audit_schema + len_on, " ");
		len_on += 1;
		strcpy(audit_schema + len_on, line_postfix);
		len_on += strlen(line_postfix);


	}
	strcpy(audit_schema + len_on, "}");
	len_on += 1;
	logmsg(LOGMSG_WARN, "audit schema: [%lu] %s\n", strlen(audit_schema), audit_schema);
	/* Assert that len_on is correct */
	assert(len_on == len);
	return audit_schema;
}
struct schema_change_type *comdb2CreateAuditTriggerScehma(Parse *parse, int dynamic, int seq, Token *proc,
                         Cdb2TrigTables *tbl){
	struct schema_change_type *sc = new_schemachange_type();

	// Guesses
    sc->onstack = 1;
	sc->type = DBTYPE_TAGGED_TABLE;
	sc->scanmode = gbl_default_sc_scanmode;
    sc->live = 1;
	// Maybe need use_plan?
	sc->addonly = 1;

	Table *pTab = tbl->table;
	char *name = pTab->zName;
	struct dbtable *db = get_dbtable_by_name(name);
	sc->newcsc2 = get_audit_schema(db, tbl->table->nCol);

	// Probably should add a dollar sign
	char *prefix = "audit_";
	int len_on = 0;
	strcpy(sc->tablename + len_on, prefix);
	len_on += strlen(prefix);
	strcpy(sc->tablename + len_on, tbl->table->zName);


    if (db->instant_schema_change) sc->instant_sc = 1;

	// What is ODH? This is just copied from timepart
	if (db->odh) sc->headers = 1;

	return sc;
}
char *gen_audited_lua(Parse *parse, Table *pTab, char *spname){
	char *code = 
"local function main(event)"
"	local audit = db:table('audit')"
"    local chg"
"    if chg == nil then"
"        chg = {}"
"    end"
"    if event.new ~= nil then"
"        for k, v in pairs(event.new) do"
"            chg['new_'..k] = v"
"        end"
"    end"
"    if event.old ~= nil then"
"        for k, v in pairs(event.old) do"
"            chg['old_'..k] = v"
"        end"
"    end"
"    chg.type = event.type"
"    chg.tbl = event.name"
"    chg.logtime = db:now()"
"    return audit:insert(chg)"
"end"
;

/*
	Got to make this work at some point
    if (comdb2TokenToStr(nm, spname, sizeof(spname))) {
        setError(pParse, SQLITE_MISUSE, "Procedure name is too long");
        logmsg(LOGMSG_WARN, "Failure on comdb2TokenToStr\n");
        return;
    }
*/

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);
    sc->addsp = 1;
	
    strcpy(sc->fname, "built-in audit");
    const char* colname[] = {"version"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
	Vdbe *v = sqlite3GetVdbe(parse);

	/*
    comdb2prepareOpFunc(v, parse, 1, sc, &comdb2ProcSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type, &stp);
						*/
	logmsg(LOGMSG_WARN, "whateves: %s, %p, %p\n", code, v, &stp);
	return code;
}

enum {
  TRIGGER_GENERIC = 1,
  TRIGGER_AUDITED = 2,
};

// dynamic -> consumer
void comdb2CreateTrigger(Parse *parse, int dynamic, Token *type, int seq, Token *proc,
                         Cdb2TrigTables *tbl)
{
	struct schema_change_type *audit_sc = comdb2CreateAuditTriggerScehma(parse, dynamic, seq, proc, tbl);

    if (comdb2IsPrepareOnly(parse))
        return;
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, dynamic ? SQLITE_CREATE_LUA_CONSUMER :
                             SQLITE_CREATE_LUA_TRIGGER, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    char spname[MAX_SPNAME];

    if (comdb2TokenToStr(proc, spname, sizeof(spname))) {
        sqlite3ErrorMsg(parse, "Procedure name is too long");
        return;
    }

	Q4SP(qname, spname);
	if (getqueuebyname(qname)) {
		sqlite3ErrorMsg(parse, "trigger already exists: %s", spname);
		return;
	}

	if (comdb2LocateSP(parse, spname) != 0) {
		return;
	}

	strbuf *s = strbuf_new();
	while (tbl) {
		Table *table = tbl->table;
		Cdb2TrigEvents *events = tbl->events;
		tbl = tbl->next;
		ColumnEventList celist;
		LIST_INIT(&celist.head);
		if (events->del.op == TK_DELETE) {
			add_watched_cols(del, table, &events->del, &celist);
		}
		if (events->ins.op == TK_INSERT) {
			add_watched_cols(ins, table, &events->ins, &celist);
		}
		if (events->upd.op == TK_UPDATE) {
			add_watched_cols(upd, table, &events->upd, &celist);
		}
		strbuf_appendf(s, "table %s\n", table->zName);
		ColumnEvent *prev = NULL, *ce = NULL;
		LIST_FOREACH(ce, &celist.head, link) {
			strbuf_appendf(s, "field %s", ce->col);
			if (ce->event & del) {
				strbuf_append(s, " del");
			}
			if (ce->event & ins) {
				strbuf_append(s, " add");
			}
			if (ce->event & upd) {
				strbuf_append(s, " pre_upd post_upd");
			}
			strbuf_append(s, "\n");
			free(prev);
			prev = ce;
		}
		free(prev);
	}

	char method[64];
	sprintf(method, "dest:%s:%s", dynamic ? "dynlua" : "lua", spname);

	// trigger add table:qname dest:method
	struct schema_change_type *sc = new_schemachange_type();
	sc->is_trigger = 1;
	sc->addonly = 1;
    sc->persistent_seq = seq;
	strcpy(sc->tablename, qname);
	struct dest *d = malloc(sizeof(struct dest));
	d->dest = strdup(method);
	listc_abl(&sc->dests, d);
	sc->newcsc2 = strbuf_disown(s);
	strbuf_free(s);
	Vdbe *v = sqlite3GetVdbe(parse);

	logmsg(LOGMSG_WARN, "Type: %s\n", type->z);

	run_internal_sql("BEGIN");

	comdb2prepareNoRows(v, parse, 0, audit_sc, &comdb2SqlSchemaChange_tran,
			    (vdbeFuncArgFree)&free_schema_change_type);
	
	comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,
			    (vdbeFuncArgFree)&free_schema_change_type);

	run_internal_sql("END");
	logmsg(LOGMSG_WARN, "commit sent\n");
				
}

void comdb2DropTrigger(Parse *parse, int dynamic, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, dynamic ? SQLITE_DROP_LUA_CONSUMER :
                             SQLITE_DROP_LUA_TRIGGER, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    char spname[MAX_SPNAME];

    if (comdb2TokenToStr(proc, spname, sizeof(spname))) {
        sqlite3ErrorMsg(parse, "Procedure name is too long");
        return;
    }

	Q4SP(qname, spname);
	if (!getqueuebyname(qname)) {
		sqlite3ErrorMsg(parse, "no such trigger: %s", spname);
		return;
	}

	// trigger drop table:qname
	struct schema_change_type *sc = new_schemachange_type();
	sc->is_trigger = 1;
	sc->drop_table = 1;
	strcpy(sc->tablename, qname);
	Vdbe *v = sqlite3GetVdbe(parse);
	comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,
			    (vdbeFuncArgFree)&free_schema_change_type);
}

#define comdb2CreateFunc(parse, proc, pfx, type)                               \
    do {                                                                       \
        char spname[MAX_SPNAME];                                               \
        if (comdb2TokenToStr(proc, spname, sizeof(spname))) {                  \
            sqlite3ErrorMsg(parse, "Procedure name is too long");              \
            return;                                                            \
        }                                                                      \
        if (comdb2LocateSP(parse, spname) != 0) {                              \
            return;                                                            \
        }                                                                      \
        if (find_lua_##pfx##func(spname)) {                                    \
            sqlite3ErrorMsg(parse, "lua " #type "func:%s already exists",      \
                            spname);                                           \
            return;                                                            \
        }                                                                      \
        struct schema_change_type *sc = new_schemachange_type();               \
        sc->is_##pfx##func = 1;                                                \
        sc->addonly = 1;                                                       \
        strcpy(sc->spname, spname);                                            \
        Vdbe *v = sqlite3GetVdbe(parse);                                       \
        comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,      \
                            (vdbeFuncArgFree)&free_schema_change_type);        \
    } while (0)

void comdb2CreateScalarFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_CREATE_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

	comdb2CreateFunc(parse, proc, s, scalar);
}

void comdb2CreateAggFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_CREATE_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

	comdb2CreateFunc(parse, proc, a, aggregate);
}

#define comdb2DropFunc(parse, proc, pfx, type)                                 \
    do {                                                                       \
        char spname[MAX_SPNAME];                                               \
        if (comdb2TokenToStr(proc, spname, sizeof(spname))) {                  \
            sqlite3ErrorMsg(parse, "Procedure name is too long");              \
            return;                                                            \
        }                                                                      \
        if (find_lua_##pfx##func(spname) == 0) {                               \
            sqlite3ErrorMsg(parse, "no such lua " #type "func:%s", spname);    \
            return;                                                            \
        }                                                                      \
        struct schema_change_type *sc = new_schemachange_type();               \
        sc->is_##pfx##func = 1;                                                \
        sc->addonly = 0;                                                       \
        strcpy(sc->spname, spname);                                            \
        Vdbe *v = sqlite3GetVdbe(parse);                                       \
        comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,      \
                            (vdbeFuncArgFree)&free_schema_change_type);        \
    } while (0)

void comdb2DropScalarFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_DROP_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

	comdb2DropFunc(parse, proc, s, scalar);
}

void comdb2DropAggFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_DROP_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

	comdb2DropFunc(parse, proc, a, aggregate);
}

