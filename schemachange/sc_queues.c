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

#include "schemachange.h"
#include "sc_queues.h"
#include "translistener.h"
#include "sc_schema.h"
#include "logmsg.h"
#include "sc_callbacks.h"
#include <math.h>

#define BDB_TRAN_MAYBE_ABORT_OR_FATAL(a,b,c) do {                             \
    (c) = 0;                                                                  \
    if (((b) != NULL) && bdb_tran_abort((a), (b), &(c)) != 0) {               \
        logmsg(LOGMSG_FATAL, "%s: bdb_tran_abort err = %d\n", __func__, (c)); \
        abort();                                                              \
    }                                                                         \
} while (0);

#define BDB_TRIGGER_MAYBE_UNPAUSE(a,b) do {                                   \
    if (((a) != NULL) && (b)) {                                               \
        int rc3 = bdb_trigger_unpause((a));                                   \
        if (rc3 == 0) {                                                       \
            (b) = 0;                                                          \
        } else {                                                              \
            logmsg(LOGMSG_ERROR, "%s: bdb_trigger_unpause rc = %d\n",         \
                   __func__, rc3);                                            \
        }                                                                     \
    }                                                                         \
} while (0);

extern int dbqueue_add_consumer(struct dbtable *db, int consumern,
                                const char *method, int noremove);

int consumer_change(const char *queuename, int consumern, const char *method)
{
    struct dbtable *db;
    int rc;

    db = getqueuebyname(queuename);
    if (!db) {
        logmsg(LOGMSG_ERROR, "no such queue '%s'\n", queuename);
        return -1;
    }

    /* Do the change.  If it works locally then assume that it will work
     * globally. */
    rc = dbqueuedb_add_consumer(db, consumern, method, 0);
    fix_consumers_with_bdblib(thedb);
    if (rc == 0) {
        rc = broadcast_add_consumer(queuename, consumern, method);
    }

    logmsg(LOGMSG_WARN, "consumer change %s-%d-%s %s\n", queuename, consumern,
           method, rc == 0 ? "SUCCESS" : "FAILED");

    if (rc == 0) {
        logmsg(LOGMSG_WARN, "**************************************\n");
        logmsg(LOGMSG_WARN, "* BE SURE TO FOLLOW UP BY MAKING THE *\n");
        logmsg(LOGMSG_WARN, "* APPROPRIATE CHANGE TO THE LRL FILE *\n");
        logmsg(LOGMSG_WARN, "* ON EACH CLUSTER NODE               *\n");
        logmsg(LOGMSG_WARN, "**************************************\n");
    }

    return rc;
}

int do_alter_queues_int(struct schema_change_type *sc)
{
    struct dbtable *db;
    int rc;
    db = getqueuebyname(sc->tablename);
    if (db == NULL) {
        if (thedb->num_qdbs >= MAX_NUM_QUEUES) {
            logmsg(LOGMSG_ERROR, "do_queue_change: too many queues\n");
            rc = -1;
        } else {
            /* create new queue */
            rc = add_queue_to_environment(sc->tablename, sc->avgitemsz,
                                          sc->pagesize);
            /* tell other nodes to follow suit */
            broadcast_add_new_queue(sc->tablename, sc->avgitemsz);
        }
    } else {
        /* TODO - change item size in existing queue */
        logmsg(LOGMSG_ERROR,
               "do_queue_change: changing existing queues not supported yet\n");
        rc = -1;
    }

    return rc;
}

void static add_to_qdbs(struct dbtable *db)
{
    thedb->qdbs =
        realloc(thedb->qdbs, (thedb->num_qdbs + 1) * sizeof(struct dbtable *));
    thedb->qdbs[thedb->num_qdbs++] = db;

    /* Add queue to the hash. */
    hash_add(thedb->qdb_hash, db);
}

int static remove_from_qdbs(struct dbtable *db)
{
    for (int i = 0; i < thedb->num_qdbs; i++) {
        if (db == thedb->qdbs[i]) {

            /* Remove the queue from the hash. */
            hash_del(thedb->qdb_hash, db);

            /* Shift the rest down one slot. */
            --thedb->num_qdbs;
            for (int j = i; j < thedb->num_qdbs; ++j) {
                thedb->qdbs[j] = thedb->qdbs[j + 1];
            }
            return 0;
        }
    }
    return -1;
}

int add_queue_to_environment(char *table, int avgitemsz, int pagesize)
{
    struct dbtable *newdb;
    int bdberr;

    /* regardless of success, the fact that we are getting asked to do this is
     * enough to indicate that any backup taken during this period may be
     * suspect. */
    gbl_sc_commit_count++;

    if (pagesize <= 0) {
        pagesize = bdb_queue_best_pagesize(avgitemsz);
        logmsg(LOGMSG_WARN,
               "Using recommended pagesize %d for avg item size %d\n", pagesize,
               avgitemsz);
    }

    newdb = newqdb(thedb, table, avgitemsz, pagesize, 0);
    if (newdb == NULL) {
        logmsg(LOGMSG_ERROR, "add_queue_to_environment:newqdb failed\n");
        return SC_INTERNAL_ERROR;
    }

    if (newdb->dbenv->master == gbl_myhostname) {
        /* I am master: create new db */
        newdb->handle =
            bdb_create_queue(newdb->tablename, thedb->basedir, avgitemsz,
                             pagesize, thedb->bdb_env, 0, &bdberr);
    } else {
        /* I am NOT master: open replicated db */
        newdb->handle =
            bdb_open_more_queue(newdb->tablename, thedb->basedir, avgitemsz,
                                pagesize, thedb->bdb_env, 0, NULL, &bdberr);
    }
    if (newdb->handle == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_open:failed to open queue %s/%s, rcode %d\n",
               thedb->basedir, newdb->tablename, bdberr);
        return SC_BDB_ERROR;
    }
    add_to_qdbs(newdb);

    return SC_OK;
}

/* We are on on replicant, being called from scdone.  just create local
 * structures (db/consumer).
 * Lots of this code is in common with master, maybe call this from
 * perform_trigger_update()? */
int perform_trigger_update_replicant(const char *queue_name, scdone_t type)
{
    struct dbtable *db = NULL;
    int rc;
    void *tran = NULL;
    char *config;
    int ndests;
    int compr;
    int persist;
    char **dests;
    uint32_t lid = 0;
    extern uint32_t gbl_rep_lockid;
    int bdberr;

    /* Queue information should already be in llmeta. Fetch it and create
     * queue/consumer handles.  Use a transaction with gbl_rep_lockid to
     * query (see comment in scdone_callback). */
    tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
    if (tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d can't begin transaction rc %d\n", __FILE__,
               __LINE__, bdberr);
        rc = bdberr;
        goto done;
    }

    bdb_get_tran_lockerid(tran, &lid);
    bdb_set_tran_lockerid(tran, gbl_rep_lockid);

    /* TODO: assert we are holding the write-lock on the queue */
    if (type != llmeta_queue_drop) {
        rc = bdb_llmeta_get_queue(tran, (char *)queue_name, &config, &ndests,
                                  &dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_llmeta_get_queue %s rc %d bdberr %d\n",
                   queue_name, rc, bdberr);
            return rc;
        }
    }

    if (type == llmeta_queue_add) {
        /* Legacy schemachange mode - we could have restarted and opened files
         * already. Make this scdone a no-op. We trust that master did
         * necessary checks before adding this queue */
        if (getqueuebyname(queue_name) != NULL) {
            rc = 0;
            goto done;
        }
        rc = javasp_do_procedure_op(JAVASP_OP_LOAD, queue_name, NULL, config);
        if (rc) {
            /* TODO: fatal error? */
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n",
                   __func__, rc);
            goto done;
        }

        db = newqdb(thedb, queue_name, 65536 /* TODO: pass from comdb2sc? */,
                    65536, 1);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "can't allocate new queue table entry\n");
            rc = -1;
            goto done;
        }
        db->handle =
            bdb_open_more_queue(queue_name, thedb->basedir, 65536, 65536,
                                thedb->bdb_env, 1, tran, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR,
                   "bdb_open:failed to open queue %s/%s, rcode %d\n",
                   thedb->basedir, db->tablename, bdberr);
            rc = -1;
            goto done;
        }
        add_to_qdbs(db);

        /* TODO: needs locking */
        rc =
            dbqueuedb_add_consumer(db, 0, dests[0] /* TODO: multiple dests */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't add consumer to queueu\n");
            rc = -1;
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 1, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }
    } else if (type == llmeta_queue_alter) {
        db = getqueuebyname(queue_name);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: %s is not a valid trigger\n", __func__,
                   queue_name);
            rc = -1;
            goto done;
        }

        /* TODO: needs locking */
        rc =
            dbqueuedb_add_consumer(db, 0, dests[0] /* TODO: multiple dests */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't add consumer to queue\n");
            rc = -1;
            goto done;
        }

        javasp_do_procedure_op(JAVASP_OP_RELOAD, queue_name, NULL, config);

        if (rc) {
            /* TODO: fatal error? */
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n",
                   __func__, rc);
            rc = -1;
            goto done;
        }

    } else if (type == llmeta_queue_drop) {
        /* get us out of database list */
        db = getqueuebyname(queue_name);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "unexpected: replicant can't find queue %s\n",
                   queue_name);
            rc = -1;
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 0, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }

        javasp_do_procedure_op(JAVASP_OP_UNLOAD, queue_name, NULL, config);

        remove_from_qdbs(db);

        /* close */
        rc = bdb_close_only(db->handle, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            rc = -1;
            goto done;
        }
    } else {
        logmsg(LOGMSG_ERROR, "unexpected trigger action %d\n", type);
        rc = -1;
        goto done;
    }

    compr = persist = 0;
    if (type != llmeta_queue_drop) {
        if (get_db_queue_odh_tran(db, &db->odh, tran) != 0 || db->odh == 0) {
            db->odh = 0;
        } else {
            get_db_queue_compress_tran(db, &compr, tran);
            get_db_queue_persistent_seq_tran(db, &persist, tran);
        }
        bdb_set_queue_odh_options(db->handle, db->odh, compr, persist);
    }

done:
    if (tran) {
        bdb_set_tran_lockerid(tran, lid);
        rc = bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_FATAL, "%s:%d failed to abort transaction\n",
                   __FILE__, __LINE__);
            exit(1);
        }
    }
    return rc;
}

static inline void set_empty_queue_options(struct schema_change_type *s)
{
    if (gbl_init_with_queue_odh == 0)
        gbl_init_with_queue_compr = 0;
    if (s->headers == -1)
        s->headers = gbl_init_with_queue_odh;
    if (s->compress == -1)
        s->compress = gbl_init_with_queue_compr;
    if (s->persistent_seq == -1)
        s->persistent_seq = gbl_init_with_queue_persistent_seq;
    if (s->compress_blobs == -1)
        s->compress_blobs = 0;
    if (s->ip_updates == -1)
        s->ip_updates = 0;
    if (s->instant_sc == -1)
        s->instant_sc = 0;
}

extern int get_physical_transaction(bdb_state_type *bdb_state,
        tran_type *logical_tran, tran_type **outtran, int force_commit);
// zTODO: IS it okay that I removed the static?
int perform_trigger_update_int(struct schema_change_type *sc)
{
    char *config = sc->newcsc2;
    int same_tran = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DONE_SAME_TRAN);
    

    /* we are on on master
     * 1) write config/destinations to llmeta
     * 2) create table in thedb->dbs
     * 3) stop/start threads for consumers, as needed
     * 4) send scdone, like any other sc
     */
    struct dbtable *db;
    tran_type *tran = NULL, *ltran = NULL;
    int rc = 0;
    int bdberr = 0;
    struct ireq iq;
    scdone_t scdone_type = llmeta_queue_add;
    SBUF2 *sb = sc->sb;

    set_empty_queue_options(sc);

    init_fake_ireq(thedb, &iq);
    iq.usedb = &thedb->static_table;

    if (same_tran) {
        rc = trans_start_logical_sc(&iq, &ltran);
        if (rc) {
            sbuf2printf(sb, "!Error %d creating logical transaction for %s.\n",
                    rc, sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
        bdb_ltran_get_schema_lock(ltran);
        rc = get_physical_transaction(thedb->bdb_env, ltran, &tran, 0);
        if (rc != 0 || tran == NULL) {
            sbuf2printf(sb, "!Error %d creating physical transaction for %s.\n",
                    rc, sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
    } else {
        rc = trans_start(&iq, NULL, (void *)&tran);
        if (rc) {
            sbuf2printf(sb, "!Error %d creating a transaction for %s.\n", rc,
                    sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }
    }

    rc = bdb_lock_tablename_write(thedb->bdb_env, "comdb2_queues", tran);
    if (rc != 0) {
        sbuf2printf(sb, "!Error %d getting tablelock for comdb2_queues.\n", rc, sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }

    rc = bdb_lock_tablename_write(thedb->bdb_env, sc->tablename, tran);
    if (rc) {
        sbuf2printf(sb, "!Error %d getting tablelock for %s.\n", rc,
                    sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }

    db = get_dbtable_by_name(sc->tablename);
    if (db) {
        sbuf2printf(sb, "!Trigger name %s clashes with existing table.\n",
                    sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        goto done;
    }
    db = getqueuebyname(sc->tablename);

    /* dropping/altering a queue that doesn't exist? */
    if ((sc->drop_table || sc->alteronly) && db == NULL) {
        sbuf2printf(sb, "!Trigger %s doesn't exist.\n", sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        rc = -1;
        goto done;
    }
    /* adding a queue that already exists? */
    else if (sc->addonly && db != NULL) {
        sbuf2printf(sb, "!Trigger %s already exists.\n", sc->tablename);
        sbuf2printf(sb, "FAILED\n");
        rc = -1;
        goto done;
    }
    if (sc->addonly) {
        if (javasp_exists(sc->tablename)) {
            sbuf2printf(sb, "!Procedure %s already exists.\n", sc->tablename);
            sbuf2printf(sb, "FAILED\n");
            rc = -1;
            goto done;
        }
        if (thedb->num_qdbs >= MAX_NUM_QUEUES) {
            sbuf2printf(sb, "!Too many queues.\n");
            sbuf2printf(sb, "FAILED\n");
            rc = -1;
            goto done;
        }
    }

    if ((rc = check_option_queue_coherency(sc, db)))
        goto done;

    char **dests;

    if (sc->addonly || sc->alteronly) {
        struct dest *d;

        dests = malloc(sizeof(char *) * sc->dests.count);
        if (dests == NULL) {
            sbuf2printf(sb, "!Can't allocate memory for destination list\n");
            logmsg(LOGMSG_ERROR,
                   "Can't allocate memory for destination list\n");
            goto done;
        }
        int i;
        for (i = 0, d = sc->dests.top; d; d = d->lnk.next, i++)
            dests[i] = d->dest;

        /* check first - backing things out gets difficult once we've done
         * things */
        if (dbqueuedb_check_consumer(dests[0])) {
            sbuf2printf(sb,
                        "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
            rc = -1;
            goto done;
        }
    }

    /* For addding, there's no queue and no consumer/procedure, etc., so create
     * those first.  For
     * other methods, we need to manage the existing consumer first. */
    if (sc->addonly) {
        rc = bdb_llmeta_add_queue(thedb->bdb_env, tran, sc->tablename, config,
                                  sc->dests.count, dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_add_queue returned %d\n",
                   __func__, rc);
            goto done;
        }

        scdone_type = llmeta_queue_add;

        db = newqdb(thedb, sc->tablename, 65536 /* TODO: pass from comdb2sc? */,
                    65536, 1);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        /* I am master: create new db */
        db->handle =
            bdb_create_queue_tran(tran, db->tablename, thedb->basedir, 65536,
                                  65536, thedb->bdb_env, 1, &bdberr);
        if (db->handle == NULL) {
            logmsg(LOGMSG_ERROR,
                   "bdb_open:failed to open queue %s/%s, rcode %d\n",
                   thedb->basedir, db->tablename, bdberr);
            goto done;
        }

        if ((rc = put_db_queue_odh(db, tran, sc->headers)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set odh for queue, rcode %d\n", rc);
            goto done;
        }

        if ((rc = put_db_queue_compress(db, tran, sc->compress)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-compression, rcode %d\n",
                   rc);
            goto done;
        }

        if ((rc = put_db_queue_persistent_seq(db, tran, sc->persistent_seq)) !=
            0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-persistent seq, rc %d\n",
                   rc);
            goto done;
        }

        if (sc->persistent_seq &&
            (rc = put_db_queue_sequence(db, tran, 0)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-sequence, rc %d\n", rc);
            goto done;
        }

        db->odh = sc->headers;
        bdb_set_queue_odh_options(db->handle, sc->headers, sc->compress,
                                  sc->persistent_seq);

        /* create a procedure (needs to go away, badly) */
        rc = javasp_do_procedure_op(JAVASP_OP_LOAD, sc->tablename, NULL, config);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: javasp_do_procedure_op returned rc %d\n", __func__, rc);
            sbuf2printf(sb, "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
            goto done;
        }

        add_to_qdbs(db);

        /* create a consumer for this guy */
        /* TODO: needs locking */
        rc = dbqueuedb_add_consumer(
            db, 0, dests[0] /* TODO: multiple destinations */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        rc = bdb_queue_consumer(db->handle, 0, 1, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_queue_consumer returned rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }
    } else if (sc->alteronly) {
        rc = bdb_llmeta_alter_queue(thedb->bdb_env, tran, sc->tablename, config,
                                    sc->dests.count, dests, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_alter_queue returned %d\n",
                   __func__, rc);
            goto done;
        }

        if ((rc = put_db_queue_odh(db, tran, sc->headers)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set odh for queue, rcode %d\n", rc);
            goto done;
        }

        if ((rc = put_db_queue_compress(db, tran, sc->compress)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-compress, rcode %d\n",
                   rc);
            goto done;
        }

        if ((rc = put_db_queue_persistent_seq(db, tran, sc->persistent_seq)) !=
            0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-persistent seq, rc %d\n",
                   rc);
            goto done;
        }

        /* Zero sequence */
        if (sc->persistent_seq &&
            (rc = put_db_queue_sequence(db, tran, 0)) != 0) {
            logmsg(LOGMSG_ERROR, "failed to set queue-sequence, rc %d\n", rc);
            goto done;
        }

        db->odh = sc->headers;
        bdb_set_queue_odh_options(db->handle, sc->headers, sc->compress,
                                  sc->persistent_seq);

        scdone_type = llmeta_queue_alter;

        /* stop */
        dbqueuedb_stop_consumers(db);
        rc = javasp_do_procedure_op(JAVASP_OP_RELOAD, db->tablename, NULL,
                                    config);
        if (rc) {
            sbuf2printf(sb,
                        "!Can't load procedure - check config/destinations?\n");
            sbuf2printf(sb, "FAILED\n");
        }

        /* TODO: needs locking */
        rc = dbqueuedb_add_consumer(
            db, 0, dests[0] /* TODO: multiple destinations */, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: newqdb returned NULL\n", __func__);
            goto done;
        }

        /* start - see the ugh above. */
        dbqueuedb_restart_consumers(db);
    } else if (sc->drop_table) {
        scdone_type = llmeta_queue_drop;
        /* stop */
        dbqueuedb_stop_consumers(db);

        /* get us out of llmeta */
        rc = bdb_llmeta_drop_queue(db->handle, tran, db->tablename, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_llmeta_drop_queue rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }

        /* close */
        rc = bdb_close_only_sc(db->handle, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }

        javasp_do_procedure_op(JAVASP_OP_UNLOAD, db->tablename, NULL, config);

        /* get us out of database list */
        remove_from_qdbs(db);
    }

    if (!same_tran) {
        rc = trans_commit(&iq, tran, gbl_myhostname);
        tran = NULL;
        if (rc) {
            sbuf2printf(sb, "!Failed to commit transaction\n");
            goto done;
        }
    }

    /* log for replicants to do the same */
    if (!same_tran) {
        rc = bdb_llog_scdone(db->handle, scdone_type, 1, &bdberr);
        if (rc) {
            sbuf2printf(sb, "!Failed to broadcast queue %s\n", sc->drop_table ? "drop" : "add");
            logmsg(LOGMSG_ERROR, "Failed to broadcast queue %s\n", sc->drop_table ? "drop" : "add");
        }
    }

    /* TODO: This is fragile - all the actions for the queue should be in one
     * transaction, including the
     * scdone. This needs to be a separate transaction right now because the
     * file handle is stil open on the
     * replicant until the scdone, and we can't delete it until it's closed. */
    if (sc->drop_table) {
        if (!same_tran) {
            rc = trans_start(&iq, NULL, (void *)&tran);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: trans_start rc %d\n", __func__, rc);
                goto done;
            }
        }

        /*
        ** NOTE: This call to bdb_get_file_version_qdb(), which ignores the
        **       returned file version number itself, is (apparently) being
        **       used to determine if the queuedb exists within llmeta.  In
        **       that case, since the first file should always be present,
        **       there should be no need to check for subsequent (optional)
        **       files?
        */
        unsigned long long ver = 0;
        if (bdb_get_file_version_qdb(db->handle, tran, 0, &ver, &bdberr) == 0) {
            sc_del_unused_files_tran(db, tran);
        } else {
            rc = bdb_del(db->handle, tran, &bdberr);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
                   __func__, rc, bdberr);
            goto done;
        }

        if (!same_tran) {
            rc = trans_commit(&iq, tran, gbl_myhostname);
            tran = NULL;
            if (rc) {
                sbuf2printf(sb, "!Failed to commit transaction\n");
                goto done;
            }
        }
    }

    if (same_tran) {
        rc = bdb_llog_scdone_tran(db->handle, scdone_type, tran, sc->tablename,
                                  &bdberr);
        if (rc) {
            sbuf2printf(sb, "!Failed write scdone , rc=%d\n", rc);
            goto done;
        }
        rc = trans_commit(&iq, ltran, gbl_myhostname);
        tran = NULL;
        ltran = NULL;
        if (rc || bdberr != BDBERR_NOERROR) {
            sbuf2printf(sb, "!Failed to commit transaction, rc=%d\n", rc);
            goto done;
        }
    }

    if (sc->addonly || sc->alteronly) {
        dbqueuedb_admin(thedb);
    }

done:
    if (ltran) bdb_tran_abort(thedb->bdb_env, ltran, &bdberr);
    else if (tran) bdb_tran_abort(thedb->bdb_env, tran, &bdberr);

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s rc:%d\n", __func__, rc);
    }
    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
    // This function does not have the "finalize" behaviour but it needs to
    // return a proper return code
    // depending on where it is being executed from.
}

int isSchemaWhitespace(char c){
	/* The ']' is here because that effectively acts as a whitespace seperator
	   So does '[', but for these purposes that is unnecessary */
	return c == ' ' || c == '\n' || c == ']';
}

const char *del_prefix = "dltd_";

// Note: this whole thing assumes you cannot change the names of columns
//  If at some point that functionality is implemented, this needs to be updated
// (I think. It might still be passable)
// zTODO: currently I am going with O(n^2) cause I don't know if we have some kind of hashset?
/*
char **get_deleted_cols(dbtable *db, dbtable *old_db){

    if (old_db == NULL){return NULL;}

    // Doing this out of the for cause generally alloca is bad in loops
    // I'm not enough of a c programmer to know of arrays have a different protocol
    // zTODO: find someone who is enough of a c programmer to know that
    char *prefix = alloca(4);
    char deleted[old_db->schema->nmembers];
    int num_deleted = 0;
    memset(deleted, 0, old_db->schema->nmembers);
    for(int i = 3; i < old_db->schema->nmembers; i++){
        struct field *old_field = db->schema->member + i;
        strncpy(prefix, old_field->name, 4);
        // Accounts for already deleted columns, as well as standard columns such as "type"
        if (strcmp(prefix, "old_") != 0 && strcmp(prefix, "new_") != 0){
            continue;
        }
        for(int j = 0; j < db->schema->nmembers; j++){
            // zTODO: right now the +4 (representing the new_ or old_) is hardcoded, which is bad
            // Also hardcodes in initialization of prefix variable
            if (strcmp(old_field->name + 4, field->name) == 0){
                continue;
            }
        }
        deleted[i] = true;
        num_deleted++;
    }
    char **deleted_cols = malloc(num_deleted * sizeof(char *));
    int del_col_ix = 0;
    for(int i = 0; i < old_db->schema->nmembers; i++){
        if(deleted[i]){
            char *name = old_db->schema->member + i;
            deleted_cols[i] = malloc((strlen(name) + 1) * sizeof(char));
            strcpy(deleted_cols[i], name);
        }
    }
    return deleted_cols;
}

*/

/* Also works for when we need to get the alter scheam */
char *get_audit_schema(dbtable *db){
	int len = 0;
	char *schema_start = "schema {cstring type[4] cstring tbl[64] datetime logtime ";
	/* "}" */
    len += strlen(schema_start);
    len += 1;
	char *line_postfix = "null=yes ";
	for(int i = 0; i < db->schema->nmembers; i++){
		struct field *entry = db->schema->member + i;
		int line_size = strlen(csc2type(entry)) + 1 + strlen(entry->name) + strlen(line_postfix);
        // zTODO: their should be a list of types that can do this
        if (entry->type == SERVER_BCSTR || entry->type == SERVER_BYTEARRAY){
            int len_size = floor(log(entry->len)) + 1;
            line_size += 2 + len_size;
        }
		int new_line = line_size + 5; /* +5 is for the "new_ " */
		int old_line = line_size + 5; /* +5 is for the "old_ " */
		len += new_line + old_line;
	}
	char *audit_schema = malloc((len + 1) * sizeof(char));
	strcpy(audit_schema, schema_start);
	for(int i = 0; i < db->schema->nmembers; i++){
		struct field *entry = db->schema->member + i;
        
		char *type = csc2type(entry);
		char *name = NULL;
        
        
        if (entry->type == SERVER_BCSTR || entry->type == SERVER_BYTEARRAY){
            int len_size = floor(log(entry->len)) + 1;
            char len[len_size + 3];
            sprintf(len, "[%d]", entry->len);
            name = malloc((strlen(entry->name) + strlen(len) + 1));
            strcpy(name, entry->name);
            strcat(name, len);
        } else {
            name = malloc((strlen(entry->name) + 1));
            strcpy(name, entry->name);
        }

		strcat(audit_schema, type);
		strcat(audit_schema, " new_");
		strcat(audit_schema, name);
		strcat(audit_schema, " ");
		strcat(audit_schema, line_postfix);

		strcat(audit_schema, type);
		strcat(audit_schema , " old_");
		strcat(audit_schema , name);
		strcat(audit_schema , " ");
		strcat(audit_schema , line_postfix);

        free(name); 

	}
    
	strcat(audit_schema, "}");
	logmsg(LOGMSG_WARN, "audit schema: [%lu] %s\n", strlen(audit_schema), audit_schema);
    assert(strlen(audit_schema) == len);
	/* Assert that len_on is correct */
	return audit_schema;
}

struct schema_change_type *comdb2CreateAuditTriggerScehma(char *name){
	struct schema_change_type *sc = new_schemachange_type();
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
	sc->newcsc2 = get_audit_schema(db);

    if (db->instant_schema_change) sc->instant_sc = 1;

	// What is ODH? This is just copied from timepart
	if (db->odh) sc->headers = 1;

	return sc;
}
struct schema_change_type *comdb2_alter_audited_sc(char *tablename, char *audit){

    struct schema_change_type *sc = new_schemachange_type();
    
    // Note: maybe there needs to be a field saying its an alter?

    // The following was gotten staight from comdb2AlterTableCSC2
    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    // Guessing shouldn't be a dry run
    sc->dryrun = 0;

    strcpy(sc->tablename, audit);

    struct dbtable *db = get_dbtable_by_name(tablename);
    sc->newcsc2 = get_audit_schema(db);

    return sc;
}

struct schema_change_type *gen_audited_lua(char *table_name, char *spname){
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
    strcpy(sc->fname, "built-in audit");
	return sc;
}

static int close_qdb(struct dbtable *db, tran_type *tran)
{
    int rc, bdberr = 0;
    assert(db->handle != NULL);
    rc = bdb_close_only_sc(db->handle, tran, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_close_only rc %d bdberr %d\n",
               __func__, rc, bdberr);
    }
    return rc;
}

static int open_qdb(struct dbtable *db, uint32_t flags, tran_type *tran)
{
    int rc, bdberr = 0;
    assert(db->handle != NULL);
    rc = bdb_open_again_tran_queue(db->handle, tran, flags, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: bdb_open_again_tran failed, bdberr %d\n",
               __func__, bdberr);
    }
    return rc;
}

int reopen_qdb(const char *queue_name, uint32_t flags, tran_type *tran)
{
    struct dbtable *db = getqueuebyname(queue_name);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no such queuedb %s\n", __func__,
               queue_name);
        return -1;
    }
    int rc, paused = 0;
    rc = bdb_trigger_pause(db->handle);
    if (rc != 0) goto done;
    paused = 1;
    rc = close_qdb(db, tran);
    if (rc != 0) goto done;
    rc = open_qdb(db, flags, tran);
    if (rc != 0) goto done;
done:
    BDB_TRIGGER_MAYBE_UNPAUSE(db->handle, paused);
    return rc;
}

static int add_qdb_file(struct schema_change_type *s, tran_type *tran)
{
    int rc, bdberr;
    SBUF2 *sb = s->sb;
    /* NOTE: The file number is hard-coded to 1 here because a queuedb is
    **       limited to having either one or two files -AND- we are adding
    **       a file, which implies there should only be one file for this
    **       queuedb at the moment, with file number 0. */
    int file_num = 1;
    unsigned long long file_version = 0;
    struct dbtable *db = getqueuebyname(s->tablename);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no such queuedb %s\n",
               __func__, s->tablename);
        sbuf2printf(sb, "!No such queuedb %s\n", s->tablename);
        rc = -1;
        goto done;
    }

    bdberr = 0;
    rc = bdb_get_file_version_qdb(db->handle, tran, file_num, &file_version,
                                  &bdberr);
    if (rc == 0 && file_version != 0) {
        logmsg(LOGMSG_ERROR,
             "%s: bdb_get_file_version_qdb rc %d name %s num %d ver %lld "
             "bdberr %d\n", __func__, rc, s->tablename, file_num, file_version,
             bdberr);
        sbuf2printf(sb,
             "!Should not find qdb %s file version %lld for file #%d\n",
             s->tablename, file_version, file_num);
        goto done;
    }
    file_version = s->qdb_file_ver;
    bdberr = 0;
    rc = bdb_new_file_version_qdb(db->handle, tran, file_num, file_version,
                                  &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR,
             "%s: bdb_new_file_version_qdb rc %d name %s num %d ver %lld "
             "bdberr %d\n", __func__, rc, s->tablename, file_num, file_version,
             bdberr);
        sbuf2printf(sb, "!Failed add qdb %s file num %d file version %lld\n",
                    s->tablename, file_num, file_version);
        goto done;
    }

done:
    logmsg(LOGMSG_INFO, "%s: %s (0x%llx) ==> %s (%d)\n",
           __func__, s->tablename, file_version,
           (rc == 0) ? "SUCCESS" : "FAILURE", rc);
    return rc;
}

static int del_qdb_file(struct schema_change_type *s, tran_type *tran)
{
    int rc, bdberr;
    struct dbtable *db;
    SBUF2 *sb = s->sb;

    db = getqueuebyname(s->tablename);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no such queuedb %s\n",
               __func__, s->tablename);
        sbuf2printf(sb, "!No such queuedb %s\n", s->tablename);
        rc = -1;
        goto done;
    }

    assert(BDB_QUEUEDB_MAX_FILES == 2); // TODO: Hard-coded for now.
    unsigned long long file_versions[BDB_QUEUEDB_MAX_FILES] = {0};
    for (int file_num = 0; file_num < BDB_QUEUEDB_MAX_FILES; file_num++) {
        bdberr = 0;
        rc = bdb_get_file_version_qdb(db->handle, tran, file_num,
                                      &file_versions[file_num], &bdberr);
        if ((rc != 0) || (file_versions[file_num] == 0)) {
            logmsg(LOGMSG_ERROR,
                 "%s: bdb_get_file_version_qdb rc %d name %s num %d ver %lld "
                 "bdberr %d\n", __func__, rc, s->tablename, file_num,
                 file_versions[file_num], bdberr);
            sbuf2printf(sb,
                 "!Bad or missing qdb %s file version %lld for file #%d\n",
                 s->tablename, file_versions[file_num], file_num);
            goto done;
        }
    }

    bdberr = 0;
    rc = bdb_new_file_version_qdb(db->handle, tran, 0, file_versions[1],
                                  &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_new_file_version_qdb rc %d err %d\n",
               __func__, rc, bdberr);
        sbuf2printf(sb, "!Failed to reset file version\n");
        goto done;
    }

    bdberr = 0;
    rc = bdb_del_file_version_qdb(db->handle, tran, 1, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_del_file_version rc %d err %d\n",
               __func__, rc, bdberr);
        sbuf2printf(sb, "!Failed to delete file version\n");
        goto done;
    }

    sc_del_unused_files_tran(db, tran);

done:
    logmsg(LOGMSG_INFO, "%s: %s (0x%llx) ==> %s (%d)\n",
           __func__, s->tablename, file_versions[0],
           (rc == 0) ? "SUCCESS" : "FAILURE", rc);
    return rc;
}

int do_add_qdb_file(struct ireq *iq, struct schema_change_type *s,
                    tran_type *tran)
{
    return 0; /* TODO: Is this even necessary? */
}

int finalize_add_qdb_file(struct ireq *iq, struct schema_change_type *s,
                          tran_type *tran)
{
    int rc, paused = 0, bdberr;
    tran_type *sc_logical_tran = NULL;
    tran_type *sc_phys_tran = NULL;

    rc = trans_start_logical_sc_with_force(iq, &sc_logical_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: trans_start_logical_sc_with_force rc %d\n",
               __func__, rc);
        goto done;
    }
    bdb_ltran_get_schema_lock(sc_logical_tran);
    if ((sc_phys_tran = bdb_get_physical_tran(sc_logical_tran)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_get_physical_tran returns NULL\n",
               __func__);
        rc = SC_FAILED_TRANSACTION;
        goto done;
    }
    rc = bdb_lock_table_write(s->db->handle, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = bdb_trigger_pause(s->db->handle);
    if (rc != 0) {
        goto done;
    }
    paused = 1;
    rc = close_qdb(s->db, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = add_qdb_file(s, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = open_qdb(s->db, BDB_OPEN_ADD_QDB_FILE, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    bdberr = 0;
    rc = bdb_llog_scdone_tran(s->db->handle, add_queue_file, sc_phys_tran,
                              NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_llog_scdone_tran rc %d bdberr %d\n",
               __func__, rc, bdberr);
        goto done;
    }
    rc = trans_commit(iq, sc_logical_tran, gbl_myhostname);
    sc_logical_tran = NULL;
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: could not commit trans\n", __func__);
        goto done;
    }
    logmsg(LOGMSG_INFO, "%s SUCCESS\n", __func__);
done:
    BDB_TRIGGER_MAYBE_UNPAUSE(s->db->handle, paused);
    BDB_TRAN_MAYBE_ABORT_OR_FATAL(s->db->handle, sc_logical_tran, bdberr);
    return rc;
}

int do_del_qdb_file(struct ireq *iq, struct schema_change_type *s,
                    tran_type *tran)
{
    return 0; /* TODO: Is this even necessary? */
}

int finalize_del_qdb_file(struct ireq *iq, struct schema_change_type *s,
                          tran_type *tran)
{
    int rc, paused = 0, bdberr;
    tran_type *sc_logical_tran = NULL;
    tran_type *sc_phys_tran = NULL;

    rc = trans_start_logical_sc_with_force(iq, &sc_logical_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: trans_start_logical_sc_with_force rc %d\n",
               __func__, rc);
        goto done;
    }
    bdb_ltran_get_schema_lock(sc_logical_tran);
    if ((sc_phys_tran = bdb_get_physical_tran(sc_logical_tran)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: bdb_get_physical_tran returns NULL\n",
               __func__);
        rc = SC_FAILED_TRANSACTION;
        goto done;
    }
    rc = bdb_lock_table_write(s->db->handle, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = bdb_trigger_pause(s->db->handle);
    if (rc != 0) {
        goto done;
    }
    paused = 1;
    rc = close_qdb(s->db, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = del_qdb_file(s, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    rc = open_qdb(s->db, BDB_OPEN_DEL_QDB_FILE, sc_phys_tran);
    if (rc != 0) {
        goto done;
    }
    bdberr = 0;
    rc = bdb_llog_scdone_tran(s->db->handle, del_queue_file, sc_phys_tran,
                              NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_llog_scdone_tran rc %d bdberr %d\n",
               __func__, rc, bdberr);
        goto done;
    }
    rc = trans_commit(iq, sc_logical_tran, gbl_myhostname);
    sc_logical_tran = NULL;
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: could not commit trans\n", __func__);
        goto done;
    }
    logmsg(LOGMSG_INFO, "%s SUCCESS\n", __func__);
done:
    BDB_TRIGGER_MAYBE_UNPAUSE(s->db->handle, paused);
    BDB_TRAN_MAYBE_ABORT_OR_FATAL(s->db->handle, sc_logical_tran, bdberr);
    return rc;
}
