/*
   Copyright 2017, 2018 Bloomberg Finance L.P.

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

#include <unistd.h>
#include "comdb2.h"
#include "comdb2_plugin.h"
#include "comdb2_appsock.h"
#include "db.h"
#include "tohex.h"

__thread SBUF2 *pgetter = NULL;

static inline void pgetter_disconnect(SBUF2 **psb)
{
    sbuf2close(*psb);
    *psb = NULL;
}

static int pgetter_connect(SBUF2 **psb)
{
    printf("^^^^^ AZ: pgetter_connect() entering\n");
    SBUF2 *sb = *psb; 
    int use_cache = 0;
    *psb = sb = connect_remote_db("getpage", thedb->envname, "getpage",
                                  "node1", use_cache);
    if (!sb) {
        logmsg(LOGMSG_ERROR, "%s unable to connect to %s\n", __func__,
               thedb->master);
        return 1;
    }

    /* we don't want timeouts so we can cache sockets on the source side...  */
    sbuf2settimeout(sb, 0, 0);

    if (sbuf2printf(sb, "getpage\n") < 0)
        abort();

    return 0;
}

/* I think we don't need a token if we are sending via appsock because
 * we will wait until we have the page back, so we don't need to implement
 * any queues to wait and notify when pages come in.
 */
int send_get_page(unsigned char fileid[DB_FILE_ID_LEN], int pageno,
                   u_int8_t *buf, size_t pagesize, size_t *niop)
{
    char expanded[DB_FILE_ID_LEN*2+1];
    util_tohex(expanded, (const char *)fileid, DB_FILE_ID_LEN);
    printf("^^^^^ AZ: %s() entering, fileid %s:%d\n", __func__, expanded, pageno);
    int count = 0;

retry:

    if (count >= 100) {
        perror("can't read response");
        abort();
        return -1;
    }

    if (!pgetter) 
        pgetter_connect(&pgetter);

    SBUF2 *sb = pgetter; 
    int rc = sbuf2fwrite((char *)fileid, 1, DB_FILE_ID_LEN, sb);
    if (rc != DB_FILE_ID_LEN) {
        abort();
        return -1;
    }

    int lltmp = htonl(pageno);
    rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
    if (rc != sizeof(lltmp)) {
        abort();
        return -1;
    }

    lltmp = htonl(pagesize);
    rc = sbuf2fwrite((char *)&lltmp, 1, sizeof(lltmp), sb);
    if (rc != sizeof(lltmp)) {
        abort();
        return -1;
    }

    rc = sbuf2flush(sb);
    if (rc <= 0) {
        abort();
        return -1;
    }

    /* we want to wait up to 1ms for page to come back */
    sbuf2settimeout(sb, 2000, 2000);

    char line[256];
    if ((rc = sbuf2gets(line, sizeof(line), sb)) < 0) {
        count++;
        printf("^^^^^ AZ: %s() line no resp rc=%d count=%d\n", __func__, rc, count);
        pgetter_disconnect(&pgetter);
        goto retry;
    }

    if ((rc = sbuf2fread((char *)buf, 1, pagesize, sb)) != pagesize) {
        count++;
        printf("^^^^^ AZ: %s() page no resp rc=%d count=%d\n", __func__, rc, count);
        pgetter_disconnect(&pgetter);
        goto retry;
    }

    printf("^^^^^ AZ: %s() got resp %s after count=%d\n", __func__, line, count);

    {
#define EXSZ 40
    char expanded[EXSZ*2+1];
    util_tohex(expanded, (const char *)buf, EXSZ);
    logmsg(LOGMSG_USER, "^^^^ data> expanded %s\n", expanded);
    }

    count = 0;
    if ((rc = sbuf2gets(line, sizeof(line), sb)) < 0) {
    //while ((rc = sbuf2fread(bptr, 1, 1, sb)) >= 0)
        count++;
        printf("^^^^^ AZ: %s() consume input rc=%d count=%d\n", __func__, rc, count);
    }
    printf("^^^^^ AZ: %s() final line %s after count=%d\n", __func__, line, count);

    *niop = pagesize;
    return 0;
}

extern int bdb_fetch_page(bdb_state_type *bdb_state, unsigned char fileid[DB_FILE_ID_LEN], int pageno, char **buf, size_t *size);

/* proper ssl exchange needs to be set up between the nodes
 * messages should be compressed by ssl layer as well */
static int handle_getpage_request(comdb2_appsock_arg_t *arg)
{
    int pageno = 0;
    int pagesize = 0;
    int rc;
    SBUF2 *sb = arg->sb;
    printf("^^^^^ AZ: %s() entering\n", __func__);
    sbuf2settimeout(sb, 0, 0);

    while (!is_sb_disconnected(sb)) {
        unsigned char fileid[DB_FILE_ID_LEN] = {0};
        if (((rc = sbuf2fread((char *)fileid, sizeof(fileid), 1, sb)) <= 0)) {
            logmsg(LOGMSG_ERROR, "%s: I/O error reading out fileid rc=%d errno=%d %s\n", __func__, rc, errno, strerror(errno));
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }

        if ((rc = sbuf2fread((char*)&pageno, sizeof(pageno), 1, sb)) <= 0) {
            logmsg(LOGMSG_ERROR, "%s: I/O error reading out pageno rc=%d\n", __func__, rc);
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }
        pageno = ntohl(pageno);

        if ((rc = sbuf2fread((char*)&pagesize, sizeof(pagesize), 1, sb)) <= 0) {
            logmsg(LOGMSG_ERROR, "%s: I/O error reading out pagesize rc=%d\n", __func__, rc);
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }
        pagesize = ntohl(pagesize);

        char expanded[DB_FILE_ID_LEN*2+1];
        util_tohex(expanded, (const char *)fileid, DB_FILE_ID_LEN);
        logmsg(LOGMSG_ERROR, "%s:REQ fileid=%s pageno=%d pagesize=%d\n", __func__, expanded, pageno, pagesize);

        if (sbuf2printf(sb, "PAGE for %llx:%d size=%d\n", fileid, pageno, pagesize) < 0 || sbuf2flush(sb) < 0) {
            logmsg(LOGMSG_ERROR, "%s: failed to send done ack text\n", __func__);
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }


        size_t loc_sz;
        /* get page content into resp->buf */
        char *bptr = malloc(pagesize);
        rc = bdb_fetch_page(thedb->bdb_env, fileid, pageno, &bptr, &loc_sz);
        if (rc || pagesize != loc_sz)
            abort();

        rc = sbuf2fwrite(bptr, 1, pagesize, sb);
        free(bptr);
        if (rc != pagesize || sbuf2flush(sb) < 0) {
            logmsg(LOGMSG_ERROR, "%s: failed to send page load rc=%d\n", __func__, rc);
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }

        if (sbuf2printf(sb, "OK\n") < 0 || sbuf2flush(sb) < 0) {
            logmsg(LOGMSG_ERROR, "%s: failed to send done ack text\n", __func__);
            arg->error = -1;
            return APPSOCK_RETURN_ERR;
        }

        printf("^^^^^ AZ: handle_getpage_request() successfully served req to get fileid=%s:%d\n", expanded, pageno);
    }

    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t getpage_plugin = {
    "getpage",             /* Name */
    "getpage fileid\npageno",                   /* Usage info */
    0,                    /* Execution count */
    0,  /* Flags */
    handle_getpage_request /* Handler function */
};

#include "plugin.h"
