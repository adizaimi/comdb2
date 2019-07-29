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

#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <alloca.h>

#include "plhash.h"
#include "intern_strings.h"

#include "mem_util.h"
#include "mem_override.h"
#include "logmsg.h"
#include "locks_wrap.h"

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_rwlock_t intern_lk;
static hash_t *interned_strings = NULL;

struct interned_string {
    char *str;
    int64_t ref;
};

static void init_interned_strings(void)
{
    interned_strings = hash_init_strptr(offsetof(struct interned_string, str));
    if (interned_strings == NULL) {
        logmsg(LOGMSG_FATAL, "can't create hash table for hostname strings\n");
        abort();
    }
    Pthread_rwlock_init(&intern_lk, NULL);
}

/* Store a copy of parameter str in a hash tbl */
char *intern(const char *str)
{
    struct interned_string *z = NULL;
    pthread_once(&once, init_interned_strings);

    Pthread_rwlock_rdlock(&intern_lk);
    struct interned_string *s = hash_find_readonly(interned_strings, &str);
    if (s == NULL) {
        /* if we did not find the entry, unlock so other threads can access hash
         * then allocate the new entry and then get the lock in write mode */
        Pthread_rwlock_unlock(&intern_lk);
        s = malloc(sizeof(struct interned_string));
        if (s == NULL) {
            return NULL;
        }
        s->str = strdup(str);
        if (s->str == NULL) {
            free(s);
            return NULL;
        }
        Pthread_rwlock_wrlock(&intern_lk);

        /* need to perform find again in case another thread inserted same */
        z = hash_find_readonly(interned_strings, &str);
        if (z == NULL) {
            hash_add(interned_strings, s);
        } else {
            /* another thread inserted same entry */
            struct interned_string *temp = s;
            s = z;
            z = temp; /* will cleanup z below */
        }
    }
    s->ref++;
    Pthread_rwlock_unlock(&intern_lk);

    if (z) { /* cleanup z: it contains item we did not insert */ 
        free(z->str);
        free(z);
    }
    return s->str;
}

char *internn(const char *str, int len)
{
    char *s;
    char *out;
    if (len > 1024)
        s = malloc(len + 1);
    else
        s = alloca(len + 1);
    memcpy(s, str, len);
    s[len] = 0;
    out = intern(s);
    if (len > 1024)
        free(s);
    return out;
}

/* return true if this is an instance of an interned string */
int isinterned(const char *node)
{
    pthread_once(&once, init_interned_strings);
    struct interned_string *s;

    Pthread_rwlock_rdlock(&intern_lk);
    s = hash_find_readonly(interned_strings, &node);
    Pthread_rwlock_unlock(&intern_lk);

    if (s && s->str == node)
        return 1;

    return 0;
}

/* lookup if given string is in the container */
int intern_find(const char *str)
{
    pthread_once(&once, init_interned_strings);
    struct interned_string *s;

    Pthread_rwlock_rdlock(&intern_lk);
    s = hash_find_readonly(interned_strings, &str);
    Pthread_rwlock_unlock(&intern_lk);

    if (s)
        return 1;

    return 0;
}


static int intern_free(void *ptr, void *unused)
{
    struct interned_string *obj = ptr;
    free(obj->str);
    obj->str = NULL;
    free(obj);
    return 0;
}

void cleanup_interned_strings()
{
    hash_for(interned_strings, intern_free, NULL);
    hash_clear(interned_strings);
    hash_free(interned_strings);
    interned_strings = NULL;
    Pthread_rwlock_destroy(&intern_lk);
}

static int intern_dump(void *ptr, void *unused)
{
    struct interned_string *obj = ptr;
    logmsg(LOGMSG_USER, "%s: str=%s %p (obj %p)\n", __func__, obj->str,
           obj->str, obj);
    return 0;
}

void dump_interned_strings()
{
    pthread_once(&once, init_interned_strings);
    hash_for(interned_strings, intern_dump, NULL);
}
