/*
 * rbd engine
 *
 * IO engine using Ceph's librbd to test RADOS Block Devices.
 *
 */

#include <rbd/librbd.h>

#include "io_ddir.h"
#include "io_u_queue.h"
#include "rbd.h"

struct fio_rbd_iou {
	struct io_u *io_u;
	rbd_completion_t completion;
	int io_seen;
	int io_complete;
};

struct rbd_data {
	rados_t cluster;
	rados_ioctx_t io_ctx;
	rbd_image_t image;
	struct io_u **aio_events;
	struct io_u **sort_events;
};

struct rbd_options {
	void *pad;
	char *rbd_name;
	char *pool_name;
	char *client_name;
	int busy_poll;
};

static int _fio_setup_rbd_data(struct thread_data *td,
			       struct rbd_data **rbd_data_ptr)
{
	struct rbd_data *rbd;

	if (td->io_ops->data)
		return 0;

	rbd = calloc(1, sizeof(struct rbd_data));
	if (!rbd)
		goto failed;

	rbd->aio_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!rbd->aio_events)
		goto failed;

	rbd->sort_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!rbd->sort_events)
		goto failed;

	*rbd_data_ptr = rbd;
	return 0;

failed:
	if (rbd)
		free(rbd);
	return 1;

}

static int _fio_rbd_connect(struct thread_data *td)
{
	struct rbd_data *rbd = td->io_ops->data;
	struct rbd_options *o = td->eo;
	int r;

	r = rados_create(&rbd->cluster, o->client_name);
	if (r < 0) {
		log_err("rados_create failed.\n");
		goto failed_early;
	}

	r = rados_conf_read_file(rbd->cluster, NULL);
	if (r < 0) {
		log_err("rados_conf_read_file failed.\n");
		goto failed_early;
	}

	r = rados_connect(rbd->cluster);
	if (r < 0) {
		log_err("rados_connect failed.\n");
		goto failed_shutdown;
	}

	r = rados_ioctx_create(rbd->cluster, o->pool_name, &rbd->io_ctx);
	if (r < 0) {
		log_err("rados_ioctx_create failed.\n");
		goto failed_shutdown;
	}

	r = rbd_open(rbd->io_ctx, o->rbd_name, &rbd->image, NULL /*snap */ );
	if (r < 0) {
		log_err("rbd_open failed.\n");
		goto failed_open;
	}
	return 0;

failed_open:
	rados_ioctx_destroy(rbd->io_ctx);
	rbd->io_ctx = NULL;
failed_shutdown:
	rados_shutdown(rbd->cluster);
	rbd->cluster = NULL;
failed_early:
	return 1;
}

static void _fio_rbd_disconnect(struct rbd_data *rbd)
{
	if (!rbd)
		return;

	/* shutdown everything */
	if (rbd->image) {
		rbd_close(rbd->image);
		rbd->image = NULL;
	}

	if (rbd->io_ctx) {
		rados_ioctx_destroy(rbd->io_ctx);
		rbd->io_ctx = NULL;
	}

	if (rbd->cluster) {
		rados_shutdown(rbd->cluster);
		rbd->cluster = NULL;
	}
}

static void _fio_rbd_finish_aiocb(rbd_completion_t comp, void *data)
{
	struct fio_rbd_iou *fri = data;
	struct io_u *io_u = fri->io_u;
	ssize_t ret;

	/*
	 * Looks like return value is 0 for success, or < 0 for
	 * a specific error. So we have to assume that it can't do
	 * partial completions.
	 */
	ret = rbd_aio_get_return_value(fri->completion);
	if (ret < 0) {
		io_u->error = ret;
		io_u->resid = io_u->xfer_buflen;
	} else
		io_u->error = 0;

	fri->io_complete = 1;
}

static struct io_u *fio_rbd_event(struct thread_data *td, int event)
{
	struct rbd_data *rbd = td->io_ops->data;

	return rbd->aio_events[event];
}

static inline int fri_check_complete(struct rbd_data *rbd, struct io_u *io_u,
				     unsigned int *events)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	if (fri->io_complete) {
		fri->io_seen = 1;
		rbd->aio_events[*events] = io_u;
		(*events)++;

		rbd_aio_release(fri->completion);
		return 1;
	}

	return 0;
}

static inline int rbd_io_u_seen(struct io_u *io_u)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	return fri->io_seen;
}

static void rbd_io_u_wait_complete(struct io_u *io_u)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	rbd_aio_wait_for_complete(fri->completion);
}

static int rbd_io_u_cmp(const void *p1, const void *p2)
{
	const struct io_u **a = (const struct io_u **) p1;
	const struct io_u **b = (const struct io_u **) p2;
	uint64_t at, bt;

	at = utime_since_now(&(*a)->start_time);
	bt = utime_since_now(&(*b)->start_time);

	if (at < bt)
		return -1;
	else if (at == bt)
		return 0;
	else
		return 1;
}

static int rbd_iter_events(struct thread_data *td, unsigned int *events,
			   unsigned int min_evts, int wait)
{
	struct rbd_data *rbd = td->io_ops->data;
	unsigned int this_events = 0;
	struct io_u *io_u;
	int i, sidx;

	sidx = 0;
	io_u_qiter(&td->io_u_all, io_u, i) {
		if (!(io_u->flags & IO_U_F_FLIGHT))
			continue;
		if (rbd_io_u_seen(io_u))
			continue;

		if (fri_check_complete(rbd, io_u, events))
			this_events++;
		else if (wait)
			rbd->sort_events[sidx++] = io_u;
	}

	if (!wait || !sidx)
		return this_events;

	/*
	 * Sort events, oldest issue first, then wait on as many as we
	 * need in order of age. If we have enough events, stop waiting,
	 * and just check if any of the older ones are done.
	 */
	if (sidx > 1)
		qsort(rbd->sort_events, sidx, sizeof(struct io_u *), rbd_io_u_cmp);

	for (i = 0; i < sidx; i++) {
		io_u = rbd->sort_events[i];

		if (fri_check_complete(rbd, io_u, events)) {
			this_events++;
			continue;
		}

		/*
		 * Stop waiting when we have enough, but continue checking
		 * all pending IOs if they are complete.
		 */
		if (*events >= min_evts)
			continue;

		rbd_io_u_wait_complete(io_u);

		if (fri_check_complete(rbd, io_u, events))
			this_events++;
	}

	return this_events;
}

static int fio_rbd_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, const struct timespec *t)
{
	unsigned int this_events, events = 0;
	struct rbd_options *o = td->eo;
	int wait = 0;

	do {
		this_events = rbd_iter_events(td, &events, min, wait);

		if (events >= min)
			break;
		if (this_events)
			continue;

		if (!o->busy_poll)
			wait = 1;
		else
			nop;
	} while (1);

	return events;
}

static int fio_rbd_queue(struct thread_data *td, struct io_u *io_u)
{
	struct rbd_data *rbd = td->io_ops->data;
	struct fio_rbd_iou *fri = io_u->engine_data;
	int r = -1;

	fio_ro_check(td, io_u);

	fri->io_seen = 0;
	fri->io_complete = 0;

	r = rbd_aio_create_completion(fri, _fio_rbd_finish_aiocb,
						&fri->completion);
	if (r < 0) {
		log_err("rbd_aio_create_completion failed.\n");
		goto failed;
	}

	if (io_u->ddir == DDIR_WRITE) {
		r = rbd_aio_write(rbd->image, io_u->offset, io_u->xfer_buflen,
					 io_u->xfer_buf, fri->completion);
		if (r < 0) {
			log_err("rbd_aio_write failed.\n");
			goto failed_comp;
		}

	} else if (io_u->ddir == DDIR_READ) {
		r = rbd_aio_read(rbd->image, io_u->offset, io_u->xfer_buflen,
					io_u->xfer_buf, fri->completion);

		if (r < 0) {
			log_err("rbd_aio_read failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_TRIM) {
		r = rbd_aio_discard(rbd->image, io_u->offset,
					io_u->xfer_buflen, fri->completion);
		if (r < 0) {
			log_err("rbd_aio_discard failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_SYNC) {
		r = rbd_aio_flush(rbd->image, fri->completion);
		if (r < 0) {
			log_err("rbd_flush failed.\n");
			goto failed_comp;
		}
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
		       io_u->ddir);
		goto failed_comp;
	}

	return FIO_Q_QUEUED;
failed_comp:
	rbd_aio_release(fri->completion);
failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_rbd_init(struct thread_data *td)
{
	int r;

	r = _fio_rbd_connect(td);
	if (r) {
		log_err("fio_rbd_connect failed, return code: %d .\n", r);
		goto failed;
	}

	return 0;

failed:
	return 1;
}

static void fio_rbd_cleanup(struct thread_data *td)
{
	struct rbd_data *rbd = td->io_ops->data;

	if (rbd) {
		_fio_rbd_disconnect(rbd);
		free(rbd->aio_events);
		free(rbd->sort_events);
		free(rbd);
	}
}

static int fio_rbd_setup(struct thread_data *td)
{
	rbd_image_info_t info;
	struct fio_file *f;
	struct rbd_data *rbd = NULL;
	int major, minor, extra;
	int r;

	/* log version of librbd. No cluster connection required. */
	rbd_version(&major, &minor, &extra);
	log_info("rbd engine: RBD version: %d.%d.%d\n", major, minor, extra);

	/* allocate engine specific structure to deal with librbd. */
	r = _fio_setup_rbd_data(td, &rbd);
	if (r) {
		log_err("fio_setup_rbd_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = rbd;

	/* librbd does not allow us to run first in the main thread and later
	 * in a fork child. It needs to be the same process context all the
	 * time. 
	 */
	td->o.use_thread = 1;

	/* connect in the main thread to determine to determine
	 * the size of the given RADOS block device. And disconnect
	 * later on.
	 */
	r = _fio_rbd_connect(td);
	if (r) {
		log_err("fio_rbd_connect failed.\n");
		goto cleanup;
	}

	/* get size of the RADOS block device */
	r = rbd_stat(rbd->image, &info, sizeof(info));
	if (r < 0) {
		log_err("rbd_status failed.\n");
		goto disconnect;
	}
	dprint(FD_IO, "rbd-engine: image size: %lu\n", info.size);

	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the RBD is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "rbd", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
		td->o.open_files++;
	}
	f = td->files[0];
	f->real_file_size = info.size;

	/* disconnect, then we were only connected to determine
	 * the size of the RBD.
	 */
	_fio_rbd_disconnect(rbd);
	return 0;

disconnect:
	_fio_rbd_disconnect(rbd);
cleanup:
	fio_rbd_cleanup(td);
	return r;
}

static int fio_rbd_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_rbd_invalidate(struct thread_data *td, struct fio_file *f)
{
#if defined(CONFIG_RBD_INVAL)
	struct rbd_data *rbd = td->io_ops->data;

	return rbd_invalidate_cache(rbd->image);
#else
	return 0;
#endif
}

static void fio_rbd_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	if (fri) {
		io_u->engine_data = NULL;
		free(fri);
	}
}

static int fio_rbd_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_rbd_iou *fri;

	fri = calloc(1, sizeof(*fri));
	fri->io_u = io_u;
	io_u->engine_data = fri;
	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "rbd",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_rbd_setup,
	.init			= fio_rbd_init,
	.queue			= fio_rbd_queue,
	.getevents		= fio_rbd_getevents,
	.event			= fio_rbd_event,
	.cleanup		= fio_rbd_cleanup,
	.open_file		= fio_rbd_open,
	.invalidate		= fio_rbd_invalidate,
	.options		= options,
	.io_u_init		= fio_rbd_io_u_init,
	.io_u_free		= fio_rbd_io_u_free,
	.option_struct_size	= sizeof(struct rbd_options),
};

static void fio_init fio_rbd_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_rbd_unregister(void)
{
	unregister_ioengine(&ioengine);
}


/* handler_rbd */

#define _GNU_SOURCE
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <endian.h>
#include <scsi/scsi.h>
#include <errno.h>

#include "tcmu-runner.h"

#ifdef ASYNC_RBD_HANDLER
#include <pthread.h>
#include <signal.h>
#include "libtcmu.h"

#define NHANDLERS 2
#define NCOMMANDS 16

struct file_handler {
	struct tcmu_device *dev;
	int num;

	pthread_mutex_t mtx;
	pthread_cond_t cond;

	pthread_t thr;
	int cmd_head;
	int cmd_tail;
	struct tcmulib_cmd *commands[NCOMMANDS];
};
#endif /* ASYNC_RBD_HANDLER */

struct file_state {
	int fd;
	uint64_t num_lbas;
	uint32_t block_size;

#ifdef ASYNC_RBD_HANDLER
	pthread_mutex_t completion_mtx;
	int curr_handler;
	struct file_handler h[NHANDLERS];
#endif /* ASYNC_RBD_HANDLER */
};

#ifdef ASYNC_RBD_HANDLER
static int file_handle_cmd(
	struct tcmu_device *dev,
	struct tcmulib_cmd *tcmulib_cmd);

static void *
file_handler_run(void *arg)
{
	struct file_handler *h = (struct file_handler *) arg;
	struct file_state *state = tcmu_get_dev_private(h->dev);

	for (;;) {
		int result;
		struct tcmulib_cmd *cmd;

		/* get next command */
		pthread_mutex_lock(&h->mtx);
		while (h->cmd_tail == h->cmd_head) {
			pthread_cond_wait(&h->cond, &h->mtx);
		}
		cmd = h->commands[h->cmd_tail];
		pthread_mutex_unlock(&h->mtx);

		/* process command */
		result = file_handle_cmd(h->dev, cmd);
		pthread_mutex_lock(&state->completion_mtx);
		tcmulib_command_complete(h->dev, cmd, result);
		tcmulib_processing_complete(h->dev);
		pthread_mutex_unlock(&state->completion_mtx);

		/* notify that we can process more commands */
		pthread_mutex_lock(&h->mtx);
		h->commands[h->cmd_tail] = NULL;
		h->cmd_tail = (h->cmd_tail + 1) % NCOMMANDS;
		pthread_cond_signal(&h->cond);
		pthread_mutex_unlock(&h->mtx);
	}

	return NULL;
}

static void
file_handler_init(struct file_handler *h, struct tcmu_device *dev, int num)
{
	int i;

	h->dev = dev;
	h->num = num;
	pthread_mutex_init(&h->mtx, NULL);
	pthread_cond_init(&h->cond, NULL);

	pthread_create(&h->thr, NULL, file_handler_run, h);
	h->cmd_head = h->cmd_tail = 0;
	for (i = 0; i < NCOMMANDS; i++)
		h->commands[i] = NULL;
}

static void
file_handler_destroy(struct file_handler *h)
{
	if (h->thr) {
		pthread_kill(h->thr, SIGINT);
		pthread_join(h->thr, NULL);
	}
	pthread_cond_destroy(&h->cond);
	pthread_mutex_destroy(&h->mtx);
}
#endif /* ASYNC_RBD_HANDLER */

static bool file_check_config(const char *cfgstring, char **reason)
{
	char *path;
	int fd;

	path = strchr(cfgstring, '/');
	if (!path) {
		asprintf(reason, "No path found");
		return false;
	}
	path += 1; /* get past '/' */

	if (access(path, W_OK) != -1)
		return true; /* File exists and is writable */

	/* We also support creating the file, so see if we can create it */
	fd = creat(path, S_IRUSR | S_IWUSR);
	if (fd == -1) {
		asprintf(reason, "Could not create file");
		return false;
	}

	unlink(path);

	return true;
}

static int file_open(struct tcmu_device *dev)
{
	struct file_state *state;
	int64_t size;
	char *config;
#ifdef ASYNC_RBD_HANDLER
	int i;
#endif /* ASYNC_RBD_HANDLER */

	state = calloc(1, sizeof(*state));
	if (!state)
		return -ENOMEM;

	tcmu_set_dev_private(dev, state);

	state->block_size = tcmu_get_attribute(dev, "hw_block_size");
	if (state->block_size == -1) {
		errp("Could not get device block size\n");
		goto err;
	}

	size = tcmu_get_device_size(dev);
	if (size == -1) {
		errp("Could not get device size\n");
		goto err;
	}

	state->num_lbas = size / state->block_size;

	config = strchr(tcmu_get_dev_cfgstring(dev), '/');
	if (!config) {
		errp("no configuration found in cfgstring\n");
		goto err;
	}
	config += 1; /* get past '/' */

	state->fd = open(config, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (state->fd == -1) {
		errp("could not open %s: %m\n", config);
		goto err;
	}

#ifdef ASYNC_RBD_HANDLER
	pthread_mutex_init(&state->completion_mtx, NULL);
	for (i = 0; i < NHANDLERS; i++)
		file_handler_init(&state->h[i], dev, i);
#endif /* ASYNC_RBD_HANDLER */

	return 0;

err:
	free(state);
	return -EINVAL;
}

static void file_close(struct tcmu_device *dev)
{
	struct file_state *state = tcmu_get_dev_private(dev);
#ifdef ASYNC_RBD_HANDLER
	int i;

	for (i = 0; i < NHANDLERS; i++)
		file_handler_destroy(&state->h[i]);
	pthread_mutex_destroy(&state->completion_mtx);
#endif /* ASYNC_RBD_HANDLER */

	close(state->fd);
	free(state);
}

static int set_medium_error(uint8_t *sense)
{
	return tcmu_set_sense_data(sense, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
}

#ifdef ASYNC_RBD_HANDLER
static int file_handle_cmd_async(
	struct tcmu_device *dev,
	struct tcmulib_cmd *tcmulib_cmd)
{
	struct file_state *state = tcmu_get_dev_private(dev);
	struct file_handler *h = &state->h[state->curr_handler];

	state->curr_handler = (state->curr_handler + 1) % NHANDLERS;

	/* enqueue command */
	pthread_mutex_lock(&h->mtx);
	while ((h->cmd_head + 1) % NCOMMANDS == h->cmd_tail) {
		pthread_cond_wait(&h->cond, &h->mtx);
	}
	h->commands[h->cmd_head] = tcmulib_cmd;
	h->cmd_head = (h->cmd_head + 1) % NCOMMANDS;
	pthread_cond_signal(&h->cond);
	pthread_mutex_unlock(&h->mtx);

	return TCMU_ASYNC_HANDLED;
}
#endif /* ASYNC_RBD_HANDLER */

/*
 * Return scsi status or TCMU_NOT_HANDLED
 */
static int file_handle_cmd(
	struct tcmu_device *dev,
	struct tcmulib_cmd *tcmulib_cmd)
{
	uint8_t *cdb = tcmulib_cmd->cdb;
	struct iovec *iovec = tcmulib_cmd->iovec;
	size_t iov_cnt = tcmulib_cmd->iov_cnt;
	uint8_t *sense = tcmulib_cmd->sense_buf;
	struct file_state *state = tcmu_get_dev_private(dev);
	uint8_t cmd;
	int remaining;
	size_t ret;

	cmd = cdb[0];

	switch (cmd) {
	case INQUIRY:
		return tcmu_emulate_inquiry(dev, cdb, iovec, iov_cnt, sense);
		break;
	case TEST_UNIT_READY:
		return tcmu_emulate_test_unit_ready(cdb, iovec, iov_cnt, sense);
		break;
	case SERVICE_ACTION_IN_16:
		if (cdb[1] == READ_CAPACITY_16)
			return tcmu_emulate_read_capacity_16(state->num_lbas,
							     state->block_size,
							     cdb, iovec, iov_cnt, sense);
		else
			return TCMU_NOT_HANDLED;
		break;
	case MODE_SENSE:
	case MODE_SENSE_10:
		return tcmu_emulate_mode_sense(cdb, iovec, iov_cnt, sense);
		break;
	case MODE_SELECT:
	case MODE_SELECT_10:
		return tcmu_emulate_mode_select(cdb, iovec, iov_cnt, sense);
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
	{
		void *buf;
		uint64_t offset = state->block_size * tcmu_get_lba(cdb);
		int length = tcmu_get_xfer_length(cdb) * state->block_size;

		/* Using this buf DTRT even if seek is beyond EOF */
		buf = malloc(length);
		if (!buf)
			return set_medium_error(sense);
		memset(buf, 0, length);

		ret = pread(state->fd, buf, length, offset);
		if (ret == -1) {
			errp("read failed: %m\n");
			free(buf);
			return set_medium_error(sense);
		}

		tcmu_memcpy_into_iovec(iovec, iov_cnt, buf, length);

		free(buf);

		return SAM_STAT_GOOD;
	}
	break;
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
	{
		uint64_t offset = state->block_size * tcmu_get_lba(cdb);
		int length = be16toh(*((uint16_t *)&cdb[7])) * state->block_size;

		remaining = length;

		while (remaining) {
			unsigned int to_copy;

			to_copy = (remaining > iovec->iov_len) ? iovec->iov_len : remaining;

			ret = pwrite(state->fd, iovec->iov_base, to_copy, offset);
			if (ret == -1) {
				errp("Could not write: %m\n");
				return set_medium_error(sense);
			}

			remaining -= to_copy;
			offset += to_copy;
			iovec++;
		}

		return SAM_STAT_GOOD;
	}
	break;
	default:
		errp("unknown command %x\n", cdb[0]);
		return TCMU_NOT_HANDLED;
	}
}

static const char file_cfg_desc[] =
	"The path to the file to use as a backstore.";

static struct tcmur_handler file_handler = {
	.cfg_desc = file_cfg_desc,

	.check_config = file_check_config,

	.open = file_open,
	.close = file_close,
#ifdef ASYNC_RBD_HANDLER
	.name = "File-backed Handler (example async code)",
	.subtype = "file_async",
	.handle_cmd = file_handle_cmd_async,
#else
	.name = "File-backed Handler (example code)",
	.subtype = "file",
	.handle_cmd = file_handle_cmd,
#endif
};

/* Entry point must be named "handler_init". */
void handler_init(void)
{
	tcmur_register_handler(&file_handler);
}
