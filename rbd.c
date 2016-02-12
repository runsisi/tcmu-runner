/*
 * rbd.c
 *
 *  Created on: Feb 9, 2016
 *      Author: runsisi AT hust.edu.cn
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <rbd/librbd.h>
#include <scsi/scsi.h>
#include "tcmu-runner.h"
#include "libtcmu.h"
#include "rbd.h"

#define NCOMMANDS 16

struct rbd_data {
	rados_t cluster;
	rados_ioctx_t ioctx;
	rbd_image_t image;
};

struct rbd_options {
	char *client_name;
	char *pool_name;
	char *image_name;
	char *snap_name;
};

struct rbd_io_handler {
	struct tcmu_device *dev;
	int io_nr;

	pthread_mutex_t cmd_mtx;
	pthread_cond_t cmd_cond;
	pthread_mutex_t io_mtx;

	pthread_t thr;
	int cmd_head;
	int cmd_tail;
	struct tcmulib_cmd *cmds[NCOMMANDS];
	struct io *ios[NCOMMANDS];
};

struct rbd_dev_state {
	struct rbd_data rbd;
	struct rbd_options options;
	uint64_t num_lbas;
	uint32_t block_size;

	pthread_mutex_t completion_mtx;

	struct rbd_io_handler h;
};

static int set_medium_error(uint8_t *sense);
static struct io *rbd_get_io(struct rbd_io_handler *h);
static void rbd_put_io(struct rbd_io_handler *h, struct io *io);
static void rbd_io_callback(rbd_completion_t comp, void *data);
static int rbd_prepare_io(struct rbd_io_handler *h, struct tcmulib_cmd *cmd, struct io *io);
static int rbd_queue_io(struct rbd_io_handler *h, struct io *io);
static struct tcmulib_cmd *rbd_get_cmd(struct rbd_io_handler *h);
static void rbd_put_cmd(struct rbd_io_handler *h, struct tcmulib_cmd *cmd, int result);
static void *rbd_io_handler_entry(void *arg);
static void rbd_io_handler_init(struct rbd_io_handler *h, struct tcmu_device *dev);
static void rbd_io_handler_destroy(struct rbd_io_handler *h);
static bool rbd_check_config(const char *cfgstring, char **reason);
static void rbd_parse_imagepath(char *path, char **pool, char **image, char **snap);
static int rbd_connect(struct rbd_dev_state *state);
static void rbd_disconnect(struct rbd_dev_state *state);
static int rbd_dev_open(struct tcmu_device *dev);
static void rbd_dev_close(struct tcmu_device *dev);
static bool rbd_can_fast_dispatch(struct tcmulib_cmd *cmd);
static int rbd_fast_dispatch(struct tcmu_device *dev,struct  tcmulib_cmd *cmd);
static int rbd_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd);
static int rbd_dispatch_cmd(struct tcmu_device *dev, struct tcmulib_cmd *cmd);

static int set_medium_error(uint8_t *sense)
{
	return tcmu_set_sense_data(sense, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
}

static struct io *rbd_get_io(struct rbd_io_handler *h)
{
	struct io *io;

	pthread_mutex_lock(&h->io_mtx);
	io = h->ios[--h->io_nr];
	h->ios[h->io_nr] = NULL;
	pthread_mutex_unlock(&h->io_mtx);

	return io;
}

static void rbd_put_io(struct rbd_io_handler *h, struct io *io)
{
	pthread_mutex_lock(&h->io_mtx);
	h->ios[h->io_nr++] = io;
	pthread_mutex_unlock(&h->io_mtx);
}

static void rbd_io_callback(rbd_completion_t comp, void *data)
{
	struct io *io = data;
	struct tcmu_device *dev = io->dev;
	struct tcmulib_cmd *cmd = io->cmd;
	struct iovec *iovec = cmd->iovec;
	size_t iov_cnt = cmd->iov_cnt;
	struct rbd_dev_state *state = tcmu_get_dev_private(dev);
	struct rbd_io_handler *h = &state->h;
	ssize_t ret;

	ret = rbd_aio_get_return_value(io->completion);
	if (ret >= 0) {
		if (io->data_op == IO_D_READ) {
			tcmu_memcpy_into_iovec(iovec, iov_cnt, io->xfer_buf, io->xfer_buflen);
		}
	}

	/* complete the scsi cmd */
	rbd_put_cmd(h, cmd, ret);

	free(io->xfer_buf);
	rbd_aio_release(io->completion);

	rbd_put_io(h, io);
}

/*
 * Return scsi status or TCMU_NOT_HANDLED
 */
static int rbd_prepare_io(struct rbd_io_handler *h, struct tcmulib_cmd *cmd, struct io *io)
{
	struct rbd_dev_state *state = tcmu_get_dev_private(h->dev);
	uint8_t *cdb = cmd->cdb;
	struct iovec *iovec = cmd->iovec;
	size_t iov_cnt = cmd->iov_cnt;
	uint8_t *sense = cmd->sense_buf;
	uint8_t scsi_cmd;

	scsi_cmd = cdb[0];

	io->cmd = cmd;

	switch (scsi_cmd) {
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
	{
		void *buf;
		uint64_t offset = state->block_size * tcmu_get_lba(cdb);
		int length = tcmu_get_xfer_length(cdb) * state->block_size;

		buf = malloc(length);
		if (!buf)
			return set_medium_error(sense);
		memset(buf, 0, length);

		io->data_op = IO_D_READ;
		io->offset = offset;
		io->xfer_buf = buf;
		io->xfer_buflen = length;

		return 0;
	}
	break;
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
	{
		void *buf;
		uint64_t offset = state->block_size * tcmu_get_lba(cdb);
		int length = be16toh(*((uint16_t *)&cdb[7])) * state->block_size;

		buf = malloc(length);
		if (!buf)
			return set_medium_error(sense);
		memset(buf, 0, length);

		io->data_op = IO_D_WRITE;
		io->offset = offset;
		io->xfer_buf = buf;
		io->xfer_buflen = length;

		tcmu_memcpy_from_iovec(io->xfer_buf, io->xfer_buflen, iovec, iov_cnt);

		return 0;
	}
	break;
	default:
		errp("unknown command %x\n", scsi_cmd);
		return TCMU_NOT_HANDLED;
	}
}

/*
 * Return scsi status or TCMU_NOT_HANDLED
 */
static int rbd_queue_io(struct rbd_io_handler *h, struct io *io)
{
	struct rbd_dev_state *state = tcmu_get_dev_private(h->dev);
	struct rbd_data *rbd = &state->rbd;
	int r = -1;

	r = rbd_aio_create_completion(io, rbd_io_callback, &io->completion);
	if (r < 0) {
		errp("rbd_aio_create_completion failed.\n");
		goto failed;
	}

	if (io->data_op == IO_D_WRITE) {
		r = rbd_aio_write(rbd->image, io->offset, io->xfer_buflen,
					 io->xfer_buf, io->completion);
		if (r < 0) {
			errp("rbd_aio_write failed.\n");
			goto failed_comp;
		}

	} else if (io->data_op == IO_D_READ) {
		r = rbd_aio_read(rbd->image, io->offset, io->xfer_buflen,
					io->xfer_buf, io->completion);

		if (r < 0) {
			errp("rbd_aio_read failed.\n");
			goto failed_comp;
		}
	} else if (io->data_op == IO_D_TRIM) {
		r = rbd_aio_discard(rbd->image, io->offset,
					io->xfer_buflen, io->completion);
		if (r < 0) {
			errp("rbd_aio_discard failed.\n");
			goto failed_comp;
		}
	} else if (io->data_op == IO_D_SYNC) {
		r = rbd_aio_flush(rbd->image, io->completion);
		if (r < 0) {
			errp("rbd_flush failed.\n");
			goto failed_comp;
		}
	} else {
		errp("%s: Warning: unhandled ddir: %d\n", __func__, io->data_op);
		goto failed_comp;
	}

	return IO_Q_QUEUED;
failed_comp:
	rbd_aio_release(io->completion);
failed:
	return IO_Q_COMPLETED;
}

static struct tcmulib_cmd *rbd_get_cmd(struct rbd_io_handler *h)
{
	struct tcmulib_cmd *cmd;

	pthread_mutex_lock(&h->cmd_mtx);
	while (h->cmd_tail == h->cmd_head) {
		pthread_cond_wait(&h->cmd_cond, &h->cmd_mtx);
	}
	cmd = h->cmds[h->cmd_tail];
	pthread_mutex_unlock(&h->cmd_mtx);

	return cmd;
}

static void rbd_put_cmd(struct rbd_io_handler *h, struct tcmulib_cmd *cmd, int result)
{
	struct rbd_dev_state *state = tcmu_get_dev_private(h->dev);

	pthread_mutex_lock(&state->completion_mtx);
	tcmulib_command_complete(h->dev, cmd, result);
	tcmulib_processing_complete(h->dev);
	pthread_mutex_unlock(&state->completion_mtx);

	/* notify that we can process more commands */
	pthread_mutex_lock(&h->cmd_mtx);
	h->cmds[h->cmd_tail] = NULL;
	h->cmd_tail = (h->cmd_tail + 1) % NCOMMANDS;
	pthread_cond_signal(&h->cmd_cond);
	pthread_mutex_unlock(&h->cmd_mtx);
}

static void *rbd_io_handler_entry(void *arg)
{
	struct rbd_io_handler *h = (struct rbd_io_handler *)arg;

	for (;;) {
		int result;
		struct tcmulib_cmd *cmd;
		struct io *io;

		/* get next command */
		cmd = rbd_get_cmd(h);

		/* get a io */
		io = rbd_get_io(h);
		assert(io);

		/* prepare io */
		result = rbd_prepare_io(h, cmd, io);
		if (result) {
			rbd_put_cmd(h, cmd, result);
			goto complete;
		}

		/* queue io */
		rbd_queue_io(h, io);

		continue;

complete:
		rbd_put_cmd(h, cmd, result);
	}

	return NULL;
}

static void rbd_io_handler_init(struct rbd_io_handler *h, struct tcmu_device *dev)
{
	int i;
	struct io *ios;

	h->dev = dev;
	h->io_nr = NCOMMANDS;
	ios = (struct io *)malloc(NCOMMANDS * sizeof(struct io));

	pthread_mutex_init(&h->cmd_mtx, NULL);
	pthread_cond_init(&h->cmd_cond, NULL);

	pthread_create(&h->thr, NULL, rbd_io_handler_entry, h);
	h->cmd_head = h->cmd_tail = 0;
	for (i = 0; i < NCOMMANDS; i++)
		h->cmds[i] = NULL;
	for (i = 0; i < NCOMMANDS; i++)
		h->ios[i] = &ios[i];
}

static void rbd_io_handler_destroy(struct rbd_io_handler *h)
{
	if (h->thr) {
		pthread_kill(h->thr, SIGINT);
		pthread_join(h->thr, NULL);
	}
	pthread_cond_destroy(&h->cmd_cond);
	pthread_mutex_destroy(&h->cmd_mtx);
}

static bool rbd_check_config(const char *cfgstring, char **reason)
{
	char *path;

	path = strchr(cfgstring, '/');
	if (!path) {
		asprintf(reason, "No path found");
		return false;
	}
	path += 1; /* get past '/' */

	/* TODO: check the rest */

	return true;
}

static void rbd_parse_imagepath(char *path, char **pool, char **image, char **snap)
{
	char *origp = strdup(path);
	char *p, *sep;

	p = origp;
	sep = strchr(p, '/');
	if (sep == NULL) {
		*pool = strdup("rbd");
	} else {
		*sep = '\0';
		*pool = strdup(p);
		p = sep + 1;
	}
	/* p points to image[@snap] */
	sep = strchr(p, '@');
	if (sep == NULL) {
		*snap = strdup("");
	} else {
		*snap = strdup(sep + 1);
		*sep = '\0';
	}
	/* p points to image\0 */
	*image = strdup(p);
	free(origp);
}

static int rbd_connect(struct rbd_dev_state *state)
{
	struct rbd_data *rbd = &state->rbd;
	struct rbd_options *opts = &state->options;
	int r;

	r = rados_create(&rbd->cluster, opts->client_name);
	if (r < 0) {
		errp("rados_create failed.\n");
		goto failed_early;
	}

	r = rados_conf_read_file(rbd->cluster, NULL);
	if (r < 0) {
		errp("rados_conf_read_file failed.\n");
		goto failed_early;
	}

	r = rados_connect(rbd->cluster);
	if (r < 0) {
		errp("rados_connect failed.\n");
		goto failed_shutdown;
	}

	r = rados_ioctx_create(rbd->cluster, opts->pool_name, &rbd->ioctx);
	if (r < 0) {
		errp("rados_ioctx_create failed.\n");
		goto failed_shutdown;
	}

	r = rbd_open(rbd->ioctx, opts->image_name, &rbd->image, NULL /*snap */ );
	if (r < 0) {
		errp("rbd_open failed.\n");
		goto failed_open;
	}
	return 0;

failed_open:
	rados_ioctx_destroy(rbd->ioctx);
	rbd->ioctx = NULL;
failed_shutdown:
	rados_shutdown(rbd->cluster);
	rbd->cluster = NULL;
failed_early:
	return 1;
}

static void rbd_disconnect(struct rbd_dev_state *state)
{
	if (!state)
		return;
	struct rbd_data *rbd = &state->rbd;

	/* shutdown everything */
	if (rbd->image) {
		rbd_close(rbd->image);
		rbd->image = NULL;
	}

	if (rbd->ioctx) {
		rados_ioctx_destroy(rbd->ioctx);
		rbd->ioctx = NULL;
	}

	if (rbd->cluster) {
		rados_shutdown(rbd->cluster);
		rbd->cluster = NULL;
	}
}

static int rbd_dev_open(struct tcmu_device *dev)
{
	struct rbd_dev_state *state;
	int64_t size;
	char *config;

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

	/* parse rbd image path */
	rbd_parse_imagepath(config,
			&state->options.pool_name,
			&state->options.image_name,
			&state->options.snap_name);

	state->options.client_name = strdup("client.admin");

	/* connect */
	if (rbd_connect(state) < 0) {
		errp("failed to connect to rbd\n");
		goto err;
	}

	pthread_mutex_init(&state->completion_mtx, NULL);

	rbd_io_handler_init(&state->h, dev);

	return 0;

err:
	free(state);
	return -EINVAL;
}

static void rbd_dev_close(struct tcmu_device *dev)
{
	struct rbd_dev_state *state = tcmu_get_dev_private(dev);

	rbd_io_handler_destroy(&state->h);

	pthread_mutex_destroy(&state->completion_mtx);

	rbd_disconnect(state);

	free(state);
}

static bool rbd_can_fast_dispatch(struct tcmulib_cmd *cmd)
{
	uint8_t *cdb = cmd->cdb;
	uint8_t scsi_cmd = cdb[0];
	bool fast_op = false;

	switch (scsi_cmd) {
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		fast_op = false;
		break;
	default:
		fast_op = true;
		break;
	}

	return fast_op;
}

static int rbd_fast_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
	uint8_t *cdb = cmd->cdb;
	struct iovec *iovec = cmd->iovec;
	size_t iov_cnt = cmd->iov_cnt;
	uint8_t *sense = cmd->sense_buf;
	struct rbd_dev_state *state = tcmu_get_dev_private(dev);
	uint8_t scsi_cmd;

	scsi_cmd = cdb[0];

	switch (scsi_cmd) {
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
	default:
		errp("unknown command %x\n", cdb[0]);
		return TCMU_NOT_HANDLED;
	}
}

static int rbd_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
	struct rbd_dev_state *state = tcmu_get_dev_private(dev);
	struct rbd_io_handler *h = &state->h;

	/* enqueue command */
	pthread_mutex_lock(&h->cmd_mtx);
	while ((h->cmd_head + 1) % NCOMMANDS == h->cmd_tail) {
		pthread_cond_wait(&h->cmd_cond, &h->cmd_mtx);
	}
	h->cmds[h->cmd_head] = cmd;
	h->cmd_head = (h->cmd_head + 1) % NCOMMANDS;
	pthread_cond_signal(&h->cmd_cond);
	pthread_mutex_unlock(&h->cmd_mtx);

	return TCMU_ASYNC_HANDLED;
}

static int rbd_dispatch_cmd(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
	if (rbd_can_fast_dispatch(cmd)) {
		return rbd_fast_dispatch(dev, cmd);
	}

	/* slow path */
	return rbd_dispatch(dev, cmd);
}

static const char rbd_cfg_desc[] =
	"The rbd image to use as a backstore.";

static struct tcmur_handler rbd_handler = {
	.cfg_desc = rbd_cfg_desc,
	.check_config = rbd_check_config,
	.open = rbd_dev_open,
	.close = rbd_dev_close,
	.name = "RBD-backed Handler (example async code)",
	.subtype = "rbd",
	.handle_cmd = rbd_dispatch_cmd,
};

/* Entry point must be named "handler_init". */
void handler_init(void)
{
	tcmur_register_handler(&rbd_handler);
}
