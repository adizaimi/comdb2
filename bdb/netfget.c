int
__memp_net_pgread(dbmfp, hp, bhp, can_create, is_recovery_page)
	DB_MPOOLFILE *dbmfp;
	DB_MPOOL_HASH *hp;
	BH *bhp;
	int can_create;
	int is_recovery_page;
{
    net_send_get_page_from_master();
    int token = put_in_circular_queue();
    if (check_status_in_circular_queue(token) == done) {
        pop_from_circular_queue(token, &result);
    }
}
 
int net_send_get_page_from_master(int fileid, int pageno, void *page)
{
    typedef struct {
        int fileid;
        int pageno;
    } req_t;
    req_t request = { .fileid = fileid, .pageno = pageno };

    uint8_t p_net_seqnum[BDB_SEQNUM_TYPE_LEN];
    int rc = 0;

    bdb_state_type *bdb_state = thedb->bdb_env;
    rc = net_send_nodrop(bdb_state->repinfo->netinfo,
            bdb_state->repinfo->master_host,
            USER_TYPE_GET_PAGE, &request,
            sizeof(request), 1);
}

int get_page_request(char *host, int fileid, int pageno)
{
    typedef struct {
        int fileid;
        int pageno;
    } req_t;

    void *buffer; 
    int bufferlen;
    get_page_into_buffer(fileid, pageno, &buffer, &bufferlen);
    
    bdb_state_type *bdb_state = thedb->bdb_env;
    rc = net_send_nodrop(bdb_state->repinfo->netinfo,
            host,
            USER_TYPE_HEREIS_PAGE, &buffer,
            bufferlen, 1);

}

int get_page_into_buffer(fileid, pageno, &buffer, &bufferlen) 
{
	__bam_read_root(dbp, txn, pgno, flags);
	return ret;
}
