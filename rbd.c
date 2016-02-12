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

#define NCOMMANDS   16
#define IODEPTH     32

struct rbd_data {
    rados_t cluster;
    rados_ioctx_t ioctx;
    rbd_image_t image;
};

struct rbd_options {
    char *cluster_name;
    char *client_name;
    char *pool_name;
    char *image_name;
    char *snap_name;
};

struct rbd_io_handler {
    struct tcmu_device *dev;

    pthread_t thr;
    pthread_mutex_t cmd_mtx;
    pthread_cond_t cmd_cond;
    pthread_mutex_t io_mtx;
    pthread_cond_t io_cond;

    int cmd_head;
    int cmd_tail;
    struct tcmulib_cmd *cmds[NCOMMANDS];

    int free_io_nr;
    struct io *free_ios[IODEPTH];
    struct io *all_ios[IODEPTH];
};

struct rbd_state {
    struct rbd_data rbd;
    struct rbd_options opts;
    uint64_t num_lbas;
    uint32_t block_size;

    pthread_mutex_t completion_mtx;

    struct rbd_io_handler h;
};

static int set_medium_error(uint8_t *sense);
static struct io *rbd_get_io(struct rbd_io_handler *h);
static void rbd_put_io(struct io *io, int r);
static void rbd_io_callback(rbd_completion_t comp, void *data);
static int rbd_setup_io(struct io *io);
static int rbd_queue_io(struct io *io);
static void *rbd_io_handler_run(void *arg);
static int rbd_io_handler_init(struct rbd_io_handler *h, struct tcmu_device *dev);
static void rbd_io_handler_destroy(struct rbd_io_handler *h);
static bool rbd_check_config(const char *cfgstring, char **reason);
static int rbd_parse_imagepath(char *path, char **pool, char **image, char **snap);
static int rbd_connect(struct rbd_state *state);
static void rbd_disconnect(struct rbd_state *state);
static int rbd_dev_open(struct tcmu_device *dev);
static void rbd_dev_close(struct tcmu_device *dev);
static bool rbd_can_fast_dispatch(struct tcmulib_cmd *cmd);
static int rbd_fast_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd);
static int rbd_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd);
static int rbd_dispatch_cmd(struct tcmu_device *dev, struct tcmulib_cmd *cmd);

static int set_medium_error(uint8_t *sense)
{
    return tcmu_set_sense_data(sense, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
}

static struct io *rbd_get_io(struct rbd_io_handler *h)
{
    struct tcmulib_cmd *cmd;
    struct io *io;

    pthread_mutex_lock(&h->cmd_mtx);
    while (h->cmd_tail == h->cmd_head) {
        pthread_cond_wait(&h->cmd_cond, &h->cmd_mtx);
    }
    cmd = h->cmds[h->cmd_tail];
    h->cmd_tail = (h->cmd_tail + 1) % NCOMMANDS;
    pthread_cond_signal(&h->cmd_cond);
    pthread_mutex_unlock(&h->cmd_mtx);

    pthread_mutex_lock(&h->io_mtx);
    while (!h->free_io_nr) {
        pthread_cond_wait(&h->io_cond, &h->io_mtx);
    }
    io = h->free_ios[--h->free_io_nr];
    pthread_mutex_unlock(&h->io_mtx);

    io->cmd = cmd;

    return io;
}

static void rbd_put_io(struct io *io, int r)
{
    struct rbd_io_handler *h = io->h;
    struct tcmulib_cmd *cmd = io->cmd;
    struct rbd_state *state = tcmu_get_dev_private(h->dev);

    pthread_mutex_lock(&state->completion_mtx);
    tcmulib_command_complete(h->dev, cmd, r);
    tcmulib_processing_complete(h->dev);
    pthread_mutex_unlock(&state->completion_mtx);

    io->flags = IO_F_FREE;
    pthread_mutex_lock(&h->io_mtx);
    h->free_ios[h->free_io_nr++] = io;
    pthread_cond_signal(&h->io_cond);
    pthread_mutex_unlock(&h->io_mtx);
}

static void rbd_io_callback(rbd_completion_t comp, void *data) {
    struct io *io = data;
    struct tcmulib_cmd *cmd = io->cmd;
    struct iovec *iovec = cmd->iovec;
    size_t iov_cnt = cmd->iov_cnt;
    uint8_t *sense = cmd->sense_buf;
    ssize_t ret;
    int r;

    ret = rbd_aio_get_return_value(io->completion);
    if (ret < 0) {
        r = set_medium_error(sense);
    } else {
        if (io->data_op == IO_D_READ) {
            tcmu_memcpy_into_iovec(iovec, iov_cnt, io->xfer_buf,
                    io->xfer_buflen);
        }
        r = 0;
    }

    free(io->xfer_buf);
    rbd_aio_release(io->completion);

    rbd_put_io(io, r);
}

/*
 * return 0 or SCSI error status
 */
static int rbd_setup_io(struct io *io)
{
    struct rbd_io_handler *h = io->h;
    struct tcmulib_cmd *cmd = io->cmd;
    struct rbd_state *state = tcmu_get_dev_private(h->dev);
    uint8_t *cdb = cmd->cdb;
    struct iovec *iovec = cmd->iovec;
    size_t iov_cnt = cmd->iov_cnt;
    uint8_t *sense = cmd->sense_buf;
    uint8_t scsi_cmd;
    void *buf;
    uint64_t offset;
    int length;
    int r;

    scsi_cmd = cdb[0];

    switch (scsi_cmd) {
    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        offset = state->block_size * tcmu_get_lba(cdb);
        length = tcmu_get_xfer_length(cdb) * state->block_size;

        buf = malloc(length);
        if (!buf) {
            r = set_medium_error(sense);
            break;
        }
        memset(buf, 0, length);

        io->data_op = IO_D_READ;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = length;

        break;
    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
        offset = state->block_size * tcmu_get_lba(cdb);
        length = be16toh(*((uint16_t *)&cdb[7])) * state->block_size;

        buf = malloc(length);
        if (!buf) {
            r = set_medium_error(sense);
            break;
        }
        memset(buf, 0, length);

        io->data_op = IO_D_WRITE;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = length;

        tcmu_memcpy_from_iovec(io->xfer_buf, io->xfer_buflen, iovec, iov_cnt);

        break;
    default:
        errp("unknown command %x\n", scsi_cmd);
        r = TCMU_NOT_HANDLED;
        assert(0);
        break;
    }

    return r;
}

/*
 * Return 0 or scsi error status
 */
static int rbd_queue_io(struct io *io)
{
    struct rbd_io_handler *h = io->h;
    struct tcmulib_cmd *cmd = io->cmd;
    struct rbd_state *state = tcmu_get_dev_private(h->dev);
    struct rbd_data *rbd = &state->rbd;
    uint8_t *sense = cmd->sense_buf;
    int ret;

    ret = rbd_aio_create_completion(io, rbd_io_callback, &io->completion);
    if (ret < 0) {
        errp("rbd_aio_create_completion failed, code: %d\n", ret);
        goto out;
    }

    if (io->data_op == IO_D_WRITE) {
        ret = rbd_aio_write(rbd->image, io->offset, io->xfer_buflen, io->xfer_buf,
                io->completion);
        if (ret < 0) {
            errp("rbd_aio_write failed, code: %d\n", ret);
            goto out_release;
        }

    } else if (io->data_op == IO_D_READ) {
        ret = rbd_aio_read(rbd->image, io->offset, io->xfer_buflen, io->xfer_buf,
                io->completion);

        if (ret < 0) {
            errp("rbd_aio_read failed, code: %d\n", ret);
            goto out_release;
        }
    } else if (io->data_op == IO_D_TRIM) {
        ret = rbd_aio_discard(rbd->image, io->offset, io->xfer_buflen,
                io->completion);
        if (ret < 0) {
            errp("rbd_aio_discard failed, code: %d\n", ret);
            goto out_release;
        }
    } else if (io->data_op == IO_D_SYNC) {
        ret = rbd_aio_flush(rbd->image, io->completion);
        if (ret < 0) {
            errp("rbd_aio_flush failed, code: %d\n", ret);
            goto out_release;
        }
    } else {
        errp("Warning: unhandled data_op: %d\n", io->data_op);
        goto out_release;
    }

    io->flags |= IO_F_FLIGHT;

    return 0;

out_release:
    rbd_aio_release(io->completion);
out:
    return set_medium_error(sense);
}

static void *rbd_io_handler_run(void *arg)
{
    struct rbd_io_handler *h = (struct rbd_io_handler *)arg;
    struct io *io;
    int r;

    for (;;) {
        /* get a io to handle */
        io = rbd_get_io(h);

        /* setup io data */
        r = rbd_setup_io(io);
        if (r) {
            rbd_put_io(io, r);
            continue;
        }

        /* queue io */
        r = rbd_queue_io(io);
        if (r) {
            rbd_put_io(io, r);
        }
    }

    return NULL;
}

static int rbd_io_handler_init(struct rbd_io_handler *h, struct tcmu_device *dev)
{
    int i;
    struct io *ios;
    int r;

    h->dev = dev;

    pthread_mutex_init(&h->cmd_mtx, NULL);
    pthread_cond_init(&h->cmd_cond, NULL);
    pthread_mutex_init(&h->io_mtx, NULL);
    pthread_cond_init(&h->io_cond, NULL);

    h->cmd_head = h->cmd_tail = 0;
    for (i = 0; i < NCOMMANDS; i++) {
        h->cmds[i] = NULL;
    }

    ios = (struct io *)malloc(IODEPTH * sizeof(struct io));
    if (!ios) {
        r = -ENOMEM;
        goto out;
    }

    h->free_io_nr = IODEPTH;
    for (i = 0; i < IODEPTH; i++) {
        h->free_ios[i] = &ios[i];
        h->all_ios[i] = &ios[i];
    }

    pthread_create(&h->thr, NULL, rbd_io_handler_run, h);

    return 0;

out:
    return r;
}

static void rbd_io_handler_destroy(struct rbd_io_handler *h)
{
    if (h->thr) {
        /* TODO: some method more gracefully */
        pthread_kill(h->thr, SIGINT);
        pthread_join(h->thr, NULL);
    }

    free(h->all_ios[0]);

    pthread_cond_destroy(&h->cmd_cond);
    pthread_mutex_destroy(&h->cmd_mtx);
    pthread_cond_destroy(&h->io_cond);
    pthread_mutex_destroy(&h->io_mtx);
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

static int rbd_parse_imagepath(char *path, char **pool, char **image, char **snap)
{
    char *origp = strdup(path);
    char *p, *sep;

    if (!origp) {
        goto out;
    }

    p = origp;
    sep = strchr(p, '/');
    if (!sep) {
        *pool = strdup("rbd");
    } else {
        *sep = '\0';
        *pool = strdup(p);
        p = ++sep;
    }
    if (!*pool) {
        goto out;
    }

    /* p points to image[@snap] */
    sep = strchr(p, '@');
    if (sep) {
        *snap = strdup(sep + 1);
        *sep = '\0';

        if (!*snap) {
            goto out;
        }
    }

    /* p points to image\0 */
    *image = strdup(p);
    if (!*image) {
        goto out;
    }

    free(origp);
    return 0;

out:
    free(pool);
    free(image);
    free(snap);
    free(origp);

    return -ENOMEM;
}

static int rbd_connect(struct rbd_state *state)
{
    struct rbd_data *rbd = &state->rbd;
    struct rbd_options *opts = &state->opts;
    int r;

    r = rados_create(&rbd->cluster, opts->client_name);
    if (r < 0) {
        errp("rados_create failed.\n");
        goto out;
    }

    r = rados_conf_read_file(rbd->cluster, NULL);
    if (r < 0) {
        errp("rados_conf_read_file failed.\n");
        goto out_shutdown;
    }

    r = rados_connect(rbd->cluster);
    if (r < 0) {
        errp("rados_connect failed.\n");
        goto out_shutdown;
    }

    r = rados_ioctx_create(rbd->cluster, opts->pool_name, &rbd->ioctx);
    if (r < 0) {
        errp("rados_ioctx_create failed.\n");
        goto out_shutdown;
    }

    r = rbd_open(rbd->ioctx, opts->image_name, &rbd->image, opts->snap_name);
    if (r < 0) {
        errp("rbd_open failed.\n");
        goto out_destroy;
    }

    return 0;

out_destroy:
    rados_ioctx_destroy(rbd->ioctx);
    rbd->ioctx = NULL;
out_shutdown:
    rados_shutdown(rbd->cluster);
    rbd->cluster = NULL;
out:
    return 1;
}

static void rbd_disconnect(struct rbd_state *state)
{
    struct rbd_data *rbd;

    if (!state)
        return;

    rbd = &state->rbd;

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
    struct rbd_state *state;
    int64_t size;
    char *config;
    int r;

    state = calloc(1, sizeof(*state));
    if (!state)
        return -ENOMEM;

    tcmu_set_dev_private(dev, state);

    state->block_size = tcmu_get_attribute(dev, "hw_block_size");
    if (state->block_size == -1) {
        errp("Could not get device block size\n");
        goto out;
    }

    size = tcmu_get_device_size(dev);
    if (size == -1) {
        errp("Could not get device size\n");
        goto out;
    }

    state->num_lbas = size / state->block_size;

    config = strchr(tcmu_get_dev_cfgstring(dev), '/');
    if (!config) {
        errp("no configuration found in cfgstring\n");
        goto out;
    }
    config += 1; /* get past '/' */

    /* parse rbd image path */
    r = rbd_parse_imagepath(config, &state->opts.pool_name,
            &state->opts.image_name, &state->opts.snap_name);
    if (r) {
        goto out;
    }

    /* TODO: parse based */
    state->opts.client_name = strdup("client.admin");

    /* connect */
    if (rbd_connect(state) < 0) {
        errp("failed to connect to rbd\n");
        goto out;
    }

    pthread_mutex_init(&state->completion_mtx, NULL);

    r = rbd_io_handler_init(&state->h, dev);
    if (r) {
        goto out;
    }

    return 0;

out:
    free(state);
    return -EINVAL;
}

static void rbd_dev_close(struct tcmu_device *dev)
{
    struct rbd_state *state = tcmu_get_dev_private(dev);
    struct rbd_options *opts = &state->opts;

    free(opts->cluster_name);
    free(opts->client_name);
    free(opts->pool_name);
    free(opts->image_name);
    free(opts->snap_name);

    rbd_disconnect(state);

    pthread_mutex_destroy(&state->completion_mtx);

    rbd_io_handler_destroy(&state->h);

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
    struct rbd_state *state = tcmu_get_dev_private(dev);
    uint8_t scsi_cmd;
    int r = TCMU_NOT_HANDLED;

    scsi_cmd = cdb[0];

    switch (scsi_cmd) {
    case INQUIRY:
        r = tcmu_emulate_inquiry(dev, cdb, iovec, iov_cnt, sense);
        break;
    case TEST_UNIT_READY:
        r = tcmu_emulate_test_unit_ready(cdb, iovec, iov_cnt, sense);
        break;
    case SERVICE_ACTION_IN_16:
        if (cdb[1] == READ_CAPACITY_16)
            r = tcmu_emulate_read_capacity_16(state->num_lbas,
                    state->block_size, cdb, iovec, iov_cnt, sense);
        break;
    case MODE_SENSE:
    case MODE_SENSE_10:
        r = tcmu_emulate_mode_sense(cdb, iovec, iov_cnt, sense);
        break;
    case MODE_SELECT:
    case MODE_SELECT_10:
        r = tcmu_emulate_mode_select(cdb, iovec, iov_cnt, sense);
        break;
    default:
        errp("unknown command %x\n", cdb[0]);
    }

    return r;
}

static int rbd_dispatch(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
    struct rbd_state *state = tcmu_get_dev_private(dev);
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

static const char rbd_cfg_desc[] = "The rbd image to use as a backstore.";

static struct tcmur_handler rbd_handler = {
    .cfg_desc = rbd_cfg_desc,
    .check_config = rbd_check_config,
    .open = rbd_dev_open,
    .close = rbd_dev_close,
    .name = "RBD-backed Handler",
    .subtype = "rbd",
    .handle_cmd = rbd_dispatch_cmd,
};

/* Entry point must be named "handler_init". */
void handler_init(void)
{
    tcmur_register_handler(&rbd_handler);
}
