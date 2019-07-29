#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "mem.h"
#include "intern_strings.h"

unsigned int iterations;


typedef struct {
    unsigned int thrid;
    unsigned int start;
    unsigned int count;
} thr_info_t;

#define MAX 1000000

void *read_thr(void *arg)
{
    thr_info_t *tinfo = (thr_info_t *)arg;

    int found = 0;
    for (int j = 0; j < MAX; j++) {
        char num[10];
        sprintf(num, "%d", j);
        
        //if (intern_find(num))
        //    found++;
        //else 
        //    abort();
    }
    printf( "Done read thr %d found %d\n", tinfo->thrid, found);
    return NULL;
}

void *write_thr(void *arg)
{
    thr_info_t *tinfo = (thr_info_t *)arg;

    for (int j = 0; j < MAX; j++) {
        char num[10];
        sprintf(num, "%d", j);
        char * ptr = intern(num);
        if(ptr == NULL)
            abort();
    }
    printf( "Done write thr %d \n", tinfo->thrid);
    return NULL;
}

/* N threads reading from 1 to MAX 
 * M threads writing from 1 to MAX
 */
int main(int argc, char *argv[])
{
    if(argc < 3) {
        fprintf(stderr, "Usage %s NUMREADTHREADS NUMWRITETHREADS ITERATIONS\n", argv[0]);
        return 1;
    }

    comdb2ma_init(0, 0);
    unsigned int numreadthreads = atoi(argv[1]);
    unsigned int numwritethreads = atoi(argv[2]);
    iterations = atoi(argv[3]);


    pthread_t *w = (pthread_t *) malloc(sizeof(pthread_t) * numwritethreads);
    thr_info_t *winfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numwritethreads);
    fprintf(stderr, "starting %d writer threads\n", numwritethreads);

    /* create threads */
    for (unsigned long long i = 0; i < numwritethreads; ++i) {
        winfo[i].thrid = i;
        pthread_create(&w[i], NULL, write_thr, (void *)&winfo[i]);
    }

    usleep(1);

    pthread_t *r = (pthread_t *) malloc(sizeof(pthread_t) * numreadthreads);
    thr_info_t *rinfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numreadthreads);
    fprintf(stderr, "starting %d reader threads\n", numreadthreads);

    /* create threads */
    for (unsigned long long i = 0; i < numreadthreads; ++i) {
        rinfo[i].thrid = i;
        pthread_create(&r[i], NULL, read_thr, (void *)&rinfo[i]);
    }

    void *wres;
    for (unsigned int i = 0; i < numwritethreads; ++i)
        pthread_join(w[i], &wres);

    void *rres;
    for (unsigned int i = 0; i < numreadthreads; ++i)
        pthread_join(r[i], &rres);

    printf("Done Main\n");
    return 0;
}
