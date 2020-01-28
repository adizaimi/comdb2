/*
   Copyright 2019, Bloomberg Finance L.P.

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


#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "logmsg.h"
#include "dyn_array.h"
#include "temptable.h"
#include "ix_return_codes.h"
#include "time_accounting.h"
#include "util.h"

size_t gbl_max_inmem_array_size = 512*1024;
size_t gbl_max_data_array_size = 512*1024;
void hexdump(loglvl lvl, const char *key, int keylen);

#if (defined _GNU_SOURCE || defined __GNU__ || defined __linux__)

static int
dyn_array_keyval_cmpr_asc(const void *p1, const void *p2, void *p3)
{
    const kv_info_t *kv1 = p1;
    const kv_info_t *kv2 = p2;
    dyn_array_t *arr = p3;
    char *keybuffer = arr->keybuffer;
    int res = 0;
    void *key1 = &keybuffer[kv1->key_start];
    void *key2 = &keybuffer[kv2->key_start];
    if (arr->compar) {
        res = arr->compar(NULL, kv1->key_len, key1, kv2->key_len, key2);
    } else {
        res = memcmp(key1, key2, kv1->key_len < kv2->key_len ? kv1->key_len : kv2->key_len);
    }
    return res;
}

#else

static int
dyn_array_keyval_cmpr_asc(const void *p1, const void *p2)
{
    const kv_info_t *kv1 = p1;
    const kv_info_t *kv2 = p2;
    dyn_array_t *arr = kv1->arr;
    char *keybuffer = arr->keybuffer;
    int res = 0;
    void *key1 = &keybuffer[kv1->key_start];
    void *key2 = &keybuffer[kv2->key_start];
    if (arr->compar) {
        res = arr->compar(NULL, kv1->key_len, key1, kv2->key_len, key2);
    } else {
        res = memcmp(key1, key2, kv1->key_len < kv2->key_len ? kv1->key_len : kv2->key_len);
    }
    return res;
}
#endif

void dyn_array_close(dyn_array_t *arr)
{
    if (!arr->is_initialized)
        return;

    if (arr->using_temp_table) {
        int bdberr;
        int rc;
        rc = bdb_temp_table_close_cursor(arr->bdb_env, arr->temp_table_cur, &bdberr);
        if (rc) abort();
        rc = bdb_temp_table_close(arr->bdb_env, arr->temp_table, &bdberr);
        if (rc) abort();
    } else {
        if (arr->kv) {
            assert(arr->capacity > 0);
            free(arr->kv);
        }
        if (arr->keybuffer) {
            assert(arr->keybuffer_capacity > 0);
            free(arr->keybuffer);
        }
        if (arr->databuffer) {
            free(arr->databuffer);
        }
        if (arr->databufferfd)
            close(arr->databufferfd);
    }
    memset(arr, 0, sizeof(dyn_array_t));
}

/* need to initialize properly with bdb_env and a comparator fuction
 * if you want this to spill to a temptable */
void dyn_array_init(dyn_array_t *arr, void *bdb_env)
{
    memset(arr, 0, sizeof(dyn_array_t));
    arr->bdb_env = bdb_env;
    arr->is_initialized = 1;
}

void dyn_array_set_cmpr(dyn_array_t *arr,
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2))
{
    arr->compar = compar;
}

int dyn_array_sort(dyn_array_t *arr)
{
    assert(arr->is_initialized);
    if (arr->using_temp_table)
        return 0; // already sorted
    if (arr->capacity <= 1) {
        assert(arr->items == 0);
        return 0; // nothing to sort
    }
    if (arr->items <= 1) {
        return 0; // nothing to sort
    }

#if (defined _GNU_SOURCE || defined __GNU__ || defined __linux__)
    qsort_r(arr->kv, arr->items, sizeof(kv_info_t), dyn_array_keyval_cmpr_asc, arr);
#else
    qsort(arr->kv, arr->items, sizeof(kv_info_t), dyn_array_keyval_cmpr_asc);
#endif
    return 0;
}

static inline struct temp_table *create_temp_table(dyn_array_t *arr)
{
    int bdberr = 0;
    struct temp_table *newtbl =
        (struct temp_table *)bdb_temp_table_create(arr->bdb_env, &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table err %d\n", bdberr);
        return NULL;
    }
    arr->temp_table = newtbl;
    bdb_temp_table_set_cmp_func(newtbl, arr->compar);
    arr->temp_table_cur = bdb_temp_table_cursor(arr->bdb_env, arr->temp_table, NULL, &bdberr);
    if (!arr->temp_table_cur)
        abort();
    return newtbl;
}


static inline void transfer_to_temp_table(dyn_array_t *arr)
{
    for (int i = 0; i < arr->items; i++) {
        //logmsg(LOGMSG_ERROR, "AZ: %d: ", i); 
        char *keybuffer = arr->keybuffer;
        kv_info_t *kv = &arr->kv[i];
        void *key = &keybuffer[kv->key_start];
        void *data = NULL;
        if(kv->data_len > 0) {
            if (!arr->data_using_file)
                data = &((char*)arr->databuffer)[kv->data_start];
            else {
                pread(arr->databufferfd, arr->databuffer, kv->data_len, kv->data_start);
                data = arr->databuffer;
            }
        }

        //printf("%d: %d %d\n", i, *(int *)key, kv->key_len);
        int bdberr;
        int rc = bdb_temp_table_insert(arr->bdb_env, arr->temp_table_cur,
                key, kv->key_len, data, kv->data_len, &bdberr);
        if (rc) abort();
    }
}

static inline int do_transfer(dyn_array_t *arr) 
{
    logmsg(LOGMSG_ERROR, "do_transfer: spill to temp table");
    assert(arr->using_temp_table == 0);
    assert(arr->temp_table == NULL);
    assert(arr->temp_table_cur == NULL);

    dyn_array_sort(arr); // sort before transfering
    arr->temp_table = create_temp_table(arr);
    if (!arr->temp_table)
        return 1;

    arr->using_temp_table = 1;
    transfer_to_temp_table(arr);
    free(arr->kv);
    if(arr->keybuffer)
        free(arr->keybuffer);
    if(arr->databufferfd) {
        close(arr->databufferfd);
    }
    if(arr->databuffer)
        free(arr->databuffer);
    arr->items = 0;
    arr->capacity = 0;
    arr->kv = NULL;
    arr->keybuffer = NULL;
    arr->databuffer = NULL;
    arr->keybuffer_capacity = 0;
    arr->databuffer_capacity = 0;
    arr->keybuffer_curr_offset = 0;
    arr->databuffer_curr_offset = 0;
    arr->databufferfd = 0;
    return 0;
}


/* a dynamic array element consists of a kv_info_t element which
 * stores the key length and start position in the keybuffer array
 * and data length and start position in the keybuffer array 
 * (start position is redundant because it is = key_start + key_len)
 */
static inline int init_internal_buffers(dyn_array_t *arr)
{
    assert(arr->items == 0);
    assert(arr->capacity == 0);
    assert(arr->keybuffer_capacity == 0);
    assert(arr->keybuffer_curr_offset == 0);
    assert(arr->databuffer_capacity == 0);
    assert(arr->databuffer_curr_offset == 0);

    arr->capacity = 1024;
    arr->kv = malloc(sizeof(*arr->kv) * arr->capacity);
    if (!arr->kv) {
        abort();
        return 1;
    }
    arr->keybuffer_capacity = 16*1024;
    arr->keybuffer = malloc(arr->keybuffer_capacity);
    if (!arr->keybuffer) {
        abort();
        free(arr->kv);
        arr->kv = NULL;
        return 1;
    }
    arr->databuffer_capacity = 32*1024;
    arr->databuffer = malloc(arr->databuffer_capacity);
    if (!arr->databuffer) {
        abort();
        free(arr->kv);
        arr->kv = NULL;
        free(arr->keybuffer);
        arr->keybuffer = NULL;
        return 1;
    }
    return 0;
}

static inline int resize_buffer(void **buffer, int *capacity, int new_offset, size_t element_sz)
{
    //printf("AZ: resize_buffer capacity =%d, new_offset = %d\n", *capacity, new_offset);
    if (*capacity <= new_offset) {
        while (*capacity <= new_offset)
            *capacity *= 2;
        void *n = realloc(*buffer, *capacity * element_sz);
        if (!n) return 1;
        *buffer = n;
    }
    return 0;
}

/* key and data get appended at the end of the keybuffer
 */
static inline int append_to_array(dyn_array_t *arr, void *key, int keylen, void *data, int datalen)
{
    int rc = 0;
    if (arr->using_temp_table) {
        abort();
    }
    if (arr->capacity == 0) {
        if(init_internal_buffers(arr))
            return 1;
    }
    assert(arr->capacity);
    assert(arr->keybuffer_capacity);
    assert(arr->databuffer_capacity);
    rc = resize_buffer((void**)&arr->kv, &arr->capacity, arr->items + 1, sizeof(*arr->kv));
    if (rc) abort();

    rc = resize_buffer(&arr->keybuffer, &arr->keybuffer_capacity, arr->keybuffer_curr_offset + keylen, sizeof(char));
    if (rc) abort();

    // assert(arr->keybuffer_capacity > new_offset);
    if (arr->keybuffer_capacity <= arr->keybuffer_curr_offset + keylen)
        abort();

    char *keybuffer = arr->keybuffer;
    void *keyloc = &keybuffer[arr->keybuffer_curr_offset];
    memcpy(keyloc, key, keylen);
    kv_info_t *kv = &arr->kv[arr->items++];
    kv->key_start = arr->keybuffer_curr_offset;
    kv->key_len = keylen;
    arr->keybuffer_curr_offset += keylen;
    kv->data_len = datalen;
    //printf("AZ: write kv at key_start =%d, keybuffer_curr_offset = %d\n", kv->key_start, arr->keybuffer_curr_offset);
#if !(defined _GNU_SOURCE || defined __GNU__ || defined __linux__)
    kv->arr = arr;
#endif

    if(datalen > 0) {
        if (!arr->data_using_file &&
            arr->databuffer_curr_offset + datalen >= gbl_max_data_array_size) {
            char *tmpfile = comdb2_location("tmp", "tempfile_XXXXXX");
            arr->databufferfd = mkstemp(tmpfile);
            if (arr->databufferfd == -1) {
                logmsg(LOGMSG_ERROR, "mkstemp rc=%d err=%s tmpfile=%s\n",
                       errno, strerror(errno), tmpfile);
            } else {
                unlink(tmpfile);
                write(arr->databufferfd, arr->databuffer, arr->databuffer_curr_offset);
                //dont free(arr->databuffer); -- will do so at close
                arr->data_using_file = 1;
            }
        }
        if (!arr->data_using_file) {
            rc = resize_buffer(&arr->databuffer, &arr->databuffer_capacity, arr->databuffer_curr_offset + datalen, sizeof(char));
            if (rc) return rc;
            if(arr->databuffer_capacity <= arr->databuffer_curr_offset + datalen)
                abort();

            char *databuffer = arr->databuffer;
            void *dataloc = &databuffer[arr->databuffer_curr_offset];
            memcpy(dataloc, data, datalen);
        } else {
            write(arr->databufferfd, data, datalen);
        }
        kv->data_start = arr->databuffer_curr_offset;
        arr->databuffer_curr_offset += datalen;
    }
    return 0;
}

int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen)
{
    assert(arr->is_initialized);
    if (!arr->using_temp_table && 
        arr->keybuffer_curr_offset > gbl_max_inmem_array_size &&
        arr->bdb_env) { // if no bdb_env we keep appending to memory
        int rc = do_transfer(arr);
        if (rc) return rc;
    }
    if (arr->using_temp_table) {
        int bdberr;
        return bdb_temp_table_insert(arr->bdb_env, arr->temp_table_cur,
                key, keylen, data, datalen, &bdberr);
    }
    return append_to_array(arr, key, keylen, data, datalen);
}

void dyn_array_dump(dyn_array_t *arr)
{
    assert(arr->is_initialized);
    for (int i = 0; i < arr->items; i++) {
        char *keybuffer = arr->keybuffer;
        kv_info_t *kv = &arr->kv[i];
        void *key = &keybuffer[kv->key_start];
        logmsg(LOGMSG_USER, "%d: %d %d\n", i, *(int *)key, kv->key_len);
    }
}

int dyn_array_first(dyn_array_t *arr)
{
    assert(arr->is_initialized);
    if (arr->using_temp_table) {
        int err;
        return bdb_temp_table_first(arr->bdb_env, arr->temp_table_cur, &err);
    }
    if (arr->items < 1)
        return IX_EMPTY;
    arr->cursor = 0;
    return IX_OK;
}

int dyn_array_next(dyn_array_t *arr)
{
    assert(arr->is_initialized);
    if (arr->using_temp_table) {
        int err;
        return bdb_temp_table_next(arr->bdb_env, arr->temp_table_cur, &err);
    }
    if (++arr->cursor >= arr->items) 
        return IX_PASTEOF;
    return IX_OK;
}

void dyn_array_get_key(dyn_array_t *arr, void **key)
{
    assert(arr->is_initialized);
    if (arr->using_temp_table) {
        *key = bdb_temp_table_key(arr->temp_table_cur);
        return;
    }
    if (arr->cursor >= arr->items) 
        abort();
    char *keybuffer = arr->keybuffer;
    kv_info_t *tmp = &arr->kv[arr->cursor];
    *key = &keybuffer[tmp->key_start];
}

void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen)
{
    assert(arr->is_initialized);
    if (arr->using_temp_table) {
        *key = bdb_temp_table_key(arr->temp_table_cur);
        *data = bdb_temp_table_data(arr->temp_table_cur);
        *datalen = bdb_temp_table_datasize(arr->temp_table_cur);
        return;
    }
    if (arr->cursor >= arr->items) 
        abort();
    char *keybuffer = arr->keybuffer;
    kv_info_t *tmp = &arr->kv[arr->cursor];
    *key = &keybuffer[tmp->key_start];
    *datalen = tmp->data_len;
    if(tmp->data_len > 0) {
        if (!arr->data_using_file)
            *data = &((char*)arr->databuffer)[tmp->data_start];
        else {
            pread(arr->databufferfd, arr->databuffer, tmp->data_len, tmp->data_start);
            *data = arr->databuffer;
        }
    }
    else 
        *data = NULL;
}

typedef struct oplog_key {
    uint16_t tbl_idx;
    uint8_t stripe;
    unsigned long long genid;
    uint8_t is_rec; // 1 for record because it needs to go after blobs
    uint32_t seq;   // record and blob will share the same seq
} oplog_key_t;


#define CMP_KEY_MEMBER(k1, k2, var)                                            \
    if (k1->var < k2->var) {                                                   \
        return -1;                                                             \
    }                                                                          \
    if (k1->var > k2->var) {                                                   \
        return 1;                                                              \
    }


int loc_osql_bplog_key_cmp(void *usermem, int key1len, const void *key1,
                       int key2len, const void *key2)
{
    assert(sizeof(oplog_key_t) == key1len);
    assert(sizeof(oplog_key_t) == key2len);

#ifdef _SUN_SOURCE
    oplog_key_t t1, t2;
    memcpy(&t1, key1, key1len);
    memcpy(&t2, key2, key2len);
    oplog_key_t *k1 = &t1;
    oplog_key_t *k2 = &t2;
#else
    oplog_key_t *k1 = (oplog_key_t *)key1;
    oplog_key_t *k2 = (oplog_key_t *)key2;
#endif

    CMP_KEY_MEMBER(k1, k2, tbl_idx);
    CMP_KEY_MEMBER(k1, k2, stripe);
    CMP_KEY_MEMBER(k1, k2, genid);
    CMP_KEY_MEMBER(k1, k2, is_rec);
    CMP_KEY_MEMBER(k1, k2, seq);

    return 0;
}

void test_one_iter_dyn_arr(int rec)
{
    dyn_array_t arr = {0};
    dyn_array_init(&arr, NULL);
    dyn_array_set_cmpr(&arr, loc_osql_bplog_key_cmp);

    oplog_key_t key = {0};
    char *value[345];

    for (int i = 0; i < rec; i++) {
        key.seq = rand();
        key.tbl_idx = (rand() % 3) + 1;
        key.is_rec = (rand() % 2);
        key.genid = rand();
        dyn_array_append(&arr, &key, sizeof(key), value, sizeof(value));
    }

    dyn_array_sort(&arr);
    //dyn_array_dump(&arr);
    dyn_array_close(&arr);
}
extern void *get_bdb_env(void);

void test_one_iter_temp_arr(int rec)
{
    oplog_key_t key = {0};
    char *value[345];
    int rc;
    int bdberr;
    struct temp_table *newtbl = bdb_temp_array_create(get_bdb_env(), &bdberr);
    bdb_temp_table_set_cmp_func(newtbl, loc_osql_bplog_key_cmp);

    for (int i = 0; i < rec; i++) {
        key.seq = rand();
        key.tbl_idx = (rand() % 3) + 1;
        key.is_rec = (rand() % 2);
        key.genid = rand();
        //rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ditk, sizeof(ditk),
                                   //data, datalen, &err);

        rc = bdb_temp_table_put(get_bdb_env(), newtbl, &key,
                sizeof(key), value, sizeof(value), NULL,
                &bdberr);
        if (rc) abort();
    }
    bdb_temp_table_close(get_bdb_env(), newtbl, &bdberr);
}

void test_dyn_array(int rec, int iter)
{
    for(int i=0; i < iter; i++) {
        srand(5);
        test_one_iter_dyn_arr(rec);
    }    
}

void test_temp_array(int rec, int iter)
{
    for(int i=0; i < iter; i++) {
        srand(5);
        test_one_iter_temp_arr(rec);
    }
}

void compare_dynarray_temparray(int rec, int iter)
{
    reset_time_accounting(CHR_TMPA);
    reset_time_accounting(CHR_TMPB);
    ACCUMULATE_TIMING(CHR_TMPA, test_dyn_array(rec, iter););
    ACCUMULATE_TIMING(CHR_TMPB, test_temp_array(rec, iter););
    print_time_accounting(CHR_TMPA);
    print_time_accounting(CHR_TMPB);
}

