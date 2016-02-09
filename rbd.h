/*
 * rbd.h
 *
 *  Created on: Feb 9, 2016
 *      Author: runsisi AT hust.edu.cn
 */

#ifndef RBD_H_
#define RBD_H_

/*
 * The io unit
 */
struct io_u {
	struct timeval start_time;
	struct timeval issue_time;

	struct fio_file *file;
	unsigned int flags;
	enum fio_ddir ddir;

	/*
	 * For replay workloads, we may want to account as a different
	 * IO type than what is being submitted.
	 */
	enum fio_ddir acct_ddir;

	/*
	 * Write generation
	 */
	unsigned short numberio;

	/*
	 * Allocated/set buffer and length
	 */
	unsigned long buflen;
	unsigned long long offset;
	void *buf;

	/*
	 * Initial seed for generating the buffer contents
	 */
	uint64_t rand_seed;

	/*
	 * IO engine state, may be different from above when we get
	 * partial transfers / residual data counts
	 */
	void *xfer_buf;
	unsigned long xfer_buflen;

	/*
	 * Parameter related to pre-filled buffers and
	 * their size to handle variable block sizes.
	 */
	unsigned long buf_filled_len;

	struct io_piece *ipo;

	unsigned int resid;
	unsigned int error;

	/*
	 * io engine private data
	 */
	union {
		unsigned int index;
		unsigned int seen;
		void *engine_data;
	};

	union {
		struct flist_head verify_list;
		struct workqueue_work work;
	};

	/*
	 * Callback for io completion
	 */
	int (*end_io)(struct thread_data *, struct io_u **);

	union {
#ifdef CONFIG_LIBAIO
		struct iocb iocb;
#endif
#ifdef CONFIG_POSIXAIO
		os_aiocb_t aiocb;
#endif
#ifdef FIO_HAVE_SGIO
		struct sg_io_hdr hdr;
#endif
#ifdef CONFIG_GUASI
		guasi_req_t greq;
#endif
#ifdef CONFIG_SOLARISAIO
		aio_result_t resultp;
#endif
#ifdef FIO_HAVE_BINJECT
		struct b_user_cmd buc;
#endif
#ifdef CONFIG_RDMA
		struct ibv_mr *mr;
#endif
		void *mmap_data;
		uint64_t null;
	};
};

/*
 * This describes a single thread/process executing a fio job.
 */
struct thread_data {
	struct thread_options o;
	struct flist_head opt_list;
	unsigned long flags;
	void *eo;
	char verror[FIO_VERROR_SIZE];
	pthread_t thread;
	unsigned int thread_number;
	unsigned int subjob_number;
	unsigned int groupid;
	struct thread_stat ts;

	int client_type;

	struct io_log *slat_log;
	struct io_log *clat_log;
	struct io_log *lat_log;
	struct io_log *bw_log;
	struct io_log *iops_log;

	struct workqueue log_compress_wq;

	struct thread_data *parent;

	uint64_t stat_io_bytes[DDIR_RWDIR_CNT];
	struct timeval bw_sample_time;

	uint64_t stat_io_blocks[DDIR_RWDIR_CNT];
	struct timeval iops_sample_time;

	/*
	 * Tracks the last iodepth number of completed writes, if data
	 * verification is enabled
	 */
	uint64_t *last_write_comp;
	unsigned int last_write_idx;

	volatile int update_rusage;
	struct fio_mutex *rusage_sem;
	struct rusage ru_start;
	struct rusage ru_end;

	struct fio_file **files;
	unsigned char *file_locks;
	unsigned int files_size;
	unsigned int files_index;
	unsigned int nr_open_files;
	unsigned int nr_done_files;
	unsigned int nr_normal_files;
	union {
		unsigned int next_file;
		struct frand_state next_file_state;
	};
	int error;
	int sig;
	int done;
	int stop_io;
	pid_t pid;
	char *orig_buffer;
	size_t orig_buffer_size;
	volatile int terminate;
	volatile int runstate;
	unsigned int last_was_sync;
	enum fio_ddir last_ddir;

	int mmapfd;

	void *iolog_buf;
	FILE *iolog_f;

	char *sysfs_root;

	unsigned long rand_seeds[FIO_RAND_NR_OFFS];

	struct frand_state bsrange_state;
	struct frand_state verify_state;
	struct frand_state trim_state;
	struct frand_state delay_state;

	struct frand_state buf_state;
	struct frand_state buf_state_prev;
	struct frand_state dedupe_state;

	unsigned int verify_batch;
	unsigned int trim_batch;

	struct thread_io_list *vstate;

	int shm_id;

	/*
	 * IO engine hooks, contains everything needed to submit an io_u
	 * to any of the available IO engines.
	 */
	struct ioengine_ops *io_ops;

	/*
	 * Queue depth of io_u's that fio MIGHT do
	 */
	unsigned int cur_depth;

	/*
	 * io_u's about to be committed
	 */
	unsigned int io_u_queued;

	/*
	 * io_u's submitted but not completed yet
	 */
	unsigned int io_u_in_flight;

	/*
	 * List of free and busy io_u's
	 */
	struct io_u_ring io_u_requeues;
	struct io_u_queue io_u_freelist;
	struct io_u_queue io_u_all;
	pthread_mutex_t io_u_lock;
	pthread_cond_t free_cond;

	/*
	 * async verify offload
	 */
	struct flist_head verify_list;
	pthread_t *verify_threads;
	unsigned int nr_verify_threads;
	pthread_cond_t verify_cond;
	int verify_thread_exit;

	/*
	 * Rate state
	 */
	uint64_t rate_bps[DDIR_RWDIR_CNT];
	unsigned long rate_next_io_time[DDIR_RWDIR_CNT];
	unsigned long rate_bytes[DDIR_RWDIR_CNT];
	unsigned long rate_blocks[DDIR_RWDIR_CNT];
	unsigned long rate_io_issue_bytes[DDIR_RWDIR_CNT];
	struct timeval lastrate[DDIR_RWDIR_CNT];
	int64_t last_usec;
	struct frand_state poisson_state;

	/*
	 * Enforced rate submission/completion workqueue
	 */
	struct workqueue io_wq;

	uint64_t total_io_size;
	uint64_t fill_device_size;

	/*
	 * Issue side
	 */
	uint64_t io_issues[DDIR_RWDIR_CNT];
	uint64_t io_issue_bytes[DDIR_RWDIR_CNT];
	uint64_t loops;

	/*
	 * Completions
	 */
	uint64_t io_blocks[DDIR_RWDIR_CNT];
	uint64_t this_io_blocks[DDIR_RWDIR_CNT];
	uint64_t io_bytes[DDIR_RWDIR_CNT];
	uint64_t this_io_bytes[DDIR_RWDIR_CNT];
	uint64_t io_skip_bytes;
	uint64_t zone_bytes;
	struct fio_mutex *mutex;
	uint64_t bytes_done[DDIR_RWDIR_CNT];

	/*
	 * State for random io, a bitmap of blocks done vs not done
	 */
	struct frand_state random_state;

	struct timeval start;	/* start of this loop */
	struct timeval epoch;	/* time job was started */
	struct timeval last_issue;
	long time_offset;
	struct timeval tv_cache;
	struct timeval terminate_time;
	unsigned int tv_cache_nr;
	unsigned int tv_cache_mask;
	unsigned int ramp_time_over;

	/*
	 * Time since last latency_window was started
	 */
	struct timeval latency_ts;
	unsigned int latency_qd;
	unsigned int latency_qd_high;
	unsigned int latency_qd_low;
	unsigned int latency_failed;
	uint64_t latency_ios;
	int latency_end_run;

	/*
	 * read/write mixed workload state
	 */
	struct frand_state rwmix_state;
	unsigned long rwmix_issues;
	enum fio_ddir rwmix_ddir;
	unsigned int ddir_seq_nr;

	/*
	 * rand/seq mixed workload state
	 */
	struct frand_state seq_rand_state[DDIR_RWDIR_CNT];

	/*
	 * IO history logs for verification. We use a tree for sorting,
	 * if we are overwriting. Otherwise just use a fifo.
	 */
	struct rb_root io_hist_tree;
	struct flist_head io_hist_list;
	unsigned long io_hist_len;

	/*
	 * For IO replaying
	 */
	struct flist_head io_log_list;

	/*
	 * For tracking/handling discards
	 */
	struct flist_head trim_list;
	unsigned long trim_entries;

	struct flist_head next_rand_list;

	/*
	 * for fileservice, how often to switch to a new file
	 */
	unsigned int file_service_nr;
	unsigned int file_service_left;
	struct fio_file *file_service_file;

	unsigned int sync_file_range_nr;

	/*
	 * For generating file sizes
	 */
	struct frand_state file_size_state;

	/*
	 * Error counts
	 */
	unsigned int total_err_count;
	int first_error;

	struct fio_flow *flow;

	/*
	 * Can be overloaded by profiles
	 */
	struct prof_io_ops prof_io_ops;
	void *prof_data;

	void *pinned_mem;
};




#endif /* RBD_H_ */
