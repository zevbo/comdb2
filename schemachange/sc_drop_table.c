/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <memory_sync.h>
#include <autoanalyze.h>
#include <translistener.h>

#include "schemachange.h"
#include "sc_drop_table.h"
#include "sc_schema.h"
#include "sc_global.h"
#include "sc_callbacks.h"

static int delete_table(struct dbtable *db, tran_type *tran)
{
    remove_constraint_pointers(db);

    int rc, bdberr;
    if ((rc = bdb_close_only_sc(db->handle, tran, &bdberr))) {
        fprintf(stderr, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    char *table = db->tablename;
    delete_db(table);
    MEMORY_SYNC;
    delete_schema(table);
    bdb_del_table_csonparameters(tran, table);
    return 0;
}

int do_drop_table(struct ireq *iq, struct schema_change_type *s,
                  tran_type *tran)
{
    struct dbtable *db;
    iq->usedb = db = s->db = get_dbtable_by_name(s->tablename);
    if (db == NULL) {
        sc_client_error(s, "Table doesn't exist");
        return SC_TABLE_DOESNOT_EXIST;
    }

    if ((!iq || iq->tranddl <= 1) && db->n_rev_constraints > 0 &&
        !self_referenced_only(db)) {
        sc_client_error(s, "Can't drop a table referenced by a foreign key");
        return -1;
    }


    char **audits;
    int num_audits;
    // zTODO: what happens if one of these fails? Do we lose our atomicity?
    bdb_get_audited_sp_tran(tran, s->tablename, &audits, &num_audits, TABLE_TO_AUDITS);
    for(int i = 0; i < num_audits; i++){
        bdb_delete_audit_table_sp_tran(tran, audits[i], 0);
    }
    bdb_delete_audited_sp_tran(tran, s->tablename, TABLE_TO_AUDITS);
    bdb_delete_audit_table_sp_tran(tran, s->tablename, 1);
    /*
    if (rc) {
        logmsg(LOGMSG_WARN, "bdb_delete_audited_sp_tran failed with rc %d\n", rc);
        return rc;
    }
    */

    return SC_OK;
}

// NB: this gets called from drop table and from fastinit
int finalize_drop_table(struct ireq *iq, struct schema_change_type *s,
                        tran_type *tran)
{
    struct dbtable *db = s->db;
    int rc = 0;
    int bdberr = 0;

    if (db->n_rev_constraints > 0 && !self_referenced_only(db)) {
        sc_client_error(s, "Can't drop a table referenced by a foreign key");
        return ERR_SC;
    }

    /* Before this handle is closed, lets wait for all the db reads to finish */
    if ((bdb_lock_tablename_write(db->handle, "comdb2_tables", tran) != 0)) {
        sc_errf(s, "%s: failed to lock comdb2_tables rc: %d\n", __func__, rc);
        return rc;
    }

    if ((bdb_lock_table_write(db->handle, tran) != 0)) {
        sc_errf(s, "%s: failed to lock table rc: %d\n", __func__, rc);
        return rc;
    }

    /* at this point if a backup is going on, it will be bad */
    gbl_sc_commit_count++;

    s->already_finalized = 1;

    if ((rc = delete_table_sequences(tran, db))) {
        sc_errf(s, "Failed deleting table sequences rc %d\n", rc);
        return rc;
    }

    delete_table(db, tran);
    /*Now that we don't have any data, please clear unwanted schemas.*/
    bdberr = bdb_reset_csc2_version(tran, db->tablename, db->schema_version);
    if (bdberr != BDBERR_NOERROR) return -1;

    if ((rc = bdb_del_file_versions(db->handle, tran, &bdberr))) {
        sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                   "%d\n",
                __func__, rc, bdberr);
        return rc;
    }

    if ((rc = table_version_upsert(db, tran, &bdberr)) != 0) {
        sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
        return rc;
    }

    if ((rc = llmeta_set_tables(tran, thedb)) != 0) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return rc;
    }

    /* Delete all access permissions related to this table. */
    if ((rc = bdb_del_all_table_access(db->handle, tran, db->tablename)) != 0)
    {
        sc_errf(s, "Failed to delete access permissions\n");
        return rc;
    }

    if (s->finalize) {
        if (create_sqlmaster_records(tran)) {
            sc_errf(s, "create_sqlmaster_records failed\n");
            return -1;
        }
        create_sqlite_master();
    }

    live_sc_off(db);

    if (!gbl_create_mode) {
        logmsg(LOGMSG_INFO, "Table %s is at version: %lld\n", db->tablename,
               db->tableversion);
    }

    if (gbl_replicate_local)
        local_replicant_write_clear(iq, tran, db);

#if 0
    /* handle in osql_scdone_commit_callback and osql_scdone_abort_callback */
    /* delete files we don't need now */
    sc_del_unused_files_tran(db, tran);
#endif

    return 0;
}
