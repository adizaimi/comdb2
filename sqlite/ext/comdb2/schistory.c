#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "schemachange.h"
#include "sc_schema.h"
#include "tohex.h"

struct sc_status_ent {
    char *name;
    char *type;
    char *newcsc2;
    char *seed;
    cdb2_client_datetime_t start;
    char *status;
    cdb2_client_datetime_t lastupdated;
    int64_t converted;
    char *error;
};

static char *status_num2str(int s)
{
    switch (s) {
    case BDB_SC_RUNNING:
        return "RUNNING";
    case BDB_SC_PAUSED:
        return "PAUSED";
    case BDB_SC_COMMITTED:
        return "COMMITTED";
    case BDB_SC_ABORTED:
        return "ABORTED";
    case BDB_SC_COMMIT_PENDING:
        return "COMMIT PENDING";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

static int get_status(void **data, int *npoints)
{
    int rc, bdberr, nkeys;
    sc_hist_row *hist = NULL;
    struct sc_status_ent *sc_status_ents = NULL;

    rc = bdb_llmeta_get_all_sc_history(NULL, &hist, &nkeys, &bdberr);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get all schema change hist\n",
               __func__);
        return SQLITE_INTERNAL;
    }

    sc_status_ents = calloc(nkeys, sizeof(struct sc_status_ent));
    if (sc_status_ents == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc\n", __func__);
        rc = SQLITE_NOMEM;
        goto cleanup;
    }

    for (int i = 0; i < nkeys; i++) {
        dttz_t d;

        //todo: sc_status_ents[i].type = strdup(get_ddl_type_str(&sc));
        sc_status_ents[i].name = strdup(hist[i].tablename);

        d = (dttz_t){.dttz_sec = hist[i].start / 1000,
                     .dttz_frac =
                         hist[i].start - (hist[i].start / 1000 * 1000),
                     .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(
            &d, "UTC", (cdb2_client_datetime_t *)&(sc_status_ents[i].start));
        d = (dttz_t){.dttz_sec = hist[i].last / 1000,
                     .dttz_frac =
                         hist[i].last - (hist[i].last / 1000 * 1000),
                     .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(
            &d, "UTC",
            (cdb2_client_datetime_t *)&(sc_status_ents[i].lastupdated));
        sc_status_ents[i].status = strdup(status_num2str(hist[i].status));

        sc_status_ents[i].converted = hist[i].converted;

        char str[22];
        sprintf(str, "%0#16"PRIx64"", flibc_htonll(hist[i].seed));

        sc_status_ents[i].seed = strdup(str);
        sc_status_ents[i].error = strdup(hist[i].errstr);
    }

    *npoints = nkeys;
    *data = sc_status_ents;

cleanup:
    free(hist);

    return rc;
}

static void free_status(void *p, int n)
{
    struct sc_status_ent *sc_status_ents = p;
    for (int i = 0; i < n; i++) {
        if (sc_status_ents[i].name)
            free(sc_status_ents[i].name);
        if (sc_status_ents[i].type)
            free(sc_status_ents[i].type);
        if (sc_status_ents[i].newcsc2)
            free(sc_status_ents[i].newcsc2);
        if (sc_status_ents[i].status)
            free(sc_status_ents[i].status);
        if (sc_status_ents[i].error)
            free(sc_status_ents[i].error);
    }
    free(sc_status_ents);
}

sqlite3_module systblScHistoryModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblScHistoryInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_sc_history", &systblScHistoryModule,
        get_status, free_status, sizeof(struct sc_status_ent),
        CDB2_CSTRING, "name", -1, offsetof(struct sc_status_ent, name),
        //CDB2_CSTRING, "type", -1, offsetof(struct sc_status_ent, type),
        //CDB2_CSTRING, "newcsc2", -1, offsetof(struct sc_status_ent, newcsc2),
        CDB2_DATETIME, "start", -1, offsetof(struct sc_status_ent, start),
        CDB2_CSTRING, "status", -1, offsetof(struct sc_status_ent, status),
        CDB2_CSTRING, "seed", -1, offsetof(struct sc_status_ent, seed),
        CDB2_DATETIME, "last_updated", -1, offsetof(struct sc_status_ent,
                                                    lastupdated),
        CDB2_INTEGER, "converted", -1, offsetof(struct sc_status_ent,
                                                converted),
        CDB2_CSTRING, "error", -1, offsetof(struct sc_status_ent, error),
        SYSTABLE_END_OF_FIELDS);
}
