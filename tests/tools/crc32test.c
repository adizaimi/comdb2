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

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <stdarg.h>
#include <alloca.h>
#include <assert.h>
#include <sys/time.h>
#include <crc32c.h>

#define N 5000000

int logmsg(int lvl, const char *fmt, ...)
{
    int ret;
    va_list args;
    va_start(args, fmt);
    ret = printf(fmt, args);
    va_end(args);
    return ret;
}

#define TOUPPER(x) (((x >= 'a') && (x <= 'z')) ? x - 32 : x)
u_int strhashfunc(u_char **keyp, int len)
{
    unsigned hash;
    u_char *key = *keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + (TOUPPER(*key));
    return hash;
}

int timediff(const char * s) {
    static struct timeval tv = {0};
    struct timeval tmp;

    gettimeofday(&tmp, NULL);
    int sec = (tmp.tv_sec - tv.tv_sec)*1000000;
    int msec = (tmp.tv_usec - tv.tv_usec);
    int usecdiff = sec + msec;
    if (tv.tv_sec != 0)
        printf("%20.20s diff = %12.dusec\n", s, usecdiff);
    tv = tmp;
    return usecdiff;
}

int crc32c_wrap(char **str, int len)
{
    return crc32c((const uint8_t *)*str, strlen(*str));
}


int main(int argc, char *argv[])
{
    crc32c_init(0);
    //compute N times crc32 checksum of small, medium, long string
    //do the same with strhashfunc

    int k;
    int l;
    char *small = "small";
    char *medium = "medium string";
    char *large = "large string of a larger size";

    timediff("starting");
    l = crc32c_wrap(&small, 5);
	for(int i = 0; i < N; i++) {
        k = crc32c_wrap(&small, 5);
        if (k != l) abort();
	}
    int csmall = timediff("crc32 small");

    l = crc32c_wrap(&medium, 5);
	for(int i = 0; i < N; i++) {
        k = crc32c_wrap(&medium, 5);
        if (k != l) abort();
	}
    int cmed = timediff("crc32 medium");

    l = crc32c_wrap(&large, 5);
	for(int i = 0; i < N; i++) {
        k = crc32c_wrap(&large, 5);
        if (k != l) abort();
	}
    int clarg = timediff("crc32 large");

    l = strhashfunc((u_char**)&small, 5);
	for(int i = 0; i < N; i++) {
        k = strhashfunc((u_char**)&small, 5);
        if (k != l) abort();
	}
    int hsmall = timediff("strhashfunc small");

    l = strhashfunc((u_char**)&medium, 5);
	for(int i = 0; i < N; i++) {
        k = strhashfunc((u_char**)&medium, 5);
        if (k != l) abort();
	}
    int hmed = timediff("strhashfunc medium");

    l = strhashfunc((u_char**)&large, 5);
	for(int i = 0; i < N; i++) {
        k = strhashfunc((u_char**)&large, 5);
        if (k != l) abort();
	}
    int hlarg = timediff("strhashfunc large");

    // for small strings strhash can be faster
    if (clarg > hlarg || cmed > hmed)
        return 1;

    return EXIT_SUCCESS;
}
