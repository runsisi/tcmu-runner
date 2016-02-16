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

#define NCOMMANDS   16
#define IODEPTH     32

enum io_data_op {
    IO_D_READ       = 0,
    IO_D_WRITE      = 1,
    IO_D_FLUSH      = 2,
    IO_D_DISCARD    = 3,
};

enum {
    IO_F_FREE       = 1 << 0,
    IO_F_PENDING    = 1 << 1,
    IO_F_INFLIGHT   = 1 << 2,
};

struct io {
    unsigned int flags;

    enum io_data_op data_op;
    unsigned long long offset;
    void *xfer_buf;
    unsigned long xfer_buflen;
    unsigned long resid;

    struct rbd_handler *h;
    struct tcmulib_cmd *cmd;
    rbd_completion_t completion;
};

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

struct rbd_handler {
    struct tcmu_device *dev;
    int stop;

    pthread_mutex_t mtx;
    pthread_mutex_t io_mtx;
    pthread_cond_t io_cond;

    int cmd_head;
    int cmd_tail;
    struct tcmulib_cmd *cmds[NCOMMANDS];

    int free_io_nr;
    struct io *free_ios[IODEPTH];
    struct io ios[IODEPTH];
};

struct rbd_state {
    struct rbd_data rbd;
    struct rbd_options opts;
    uint64_t num_lbas;
    uint32_t block_size;

    /*
     * The below will all be 0 because the handler is now responsible for
     * enabling support, and this handler does not yet do so.
     */

    /* write caching supported */
    bool wce;
    /* logical block provisioning (UNMAP) supported */
    bool tpu;
    /* logical block provisioning (WRITE_SAME) supported */
    bool tpws;

    pthread_mutex_t completion_mtx;

    struct rbd_handler h;
};

static int rbd_do_io(struct io *io);

static int set_medium_error(uint8_t *sense)
{
    return tcmu_set_sense_data(sense, MEDIUM_ERROR, ASC_READ_ERROR, NULL);
}

static void rbd_complete_cmd(struct rbd_handler *h, struct tcmulib_cmd *cmd, int r)
{
    struct rbd_state *state = tcmu_get_dev_private(h->dev);

    pthread_mutex_lock(&state->completion_mtx);
    tcmulib_command_complete(h->dev, cmd, r);
    tcmulib_processing_complete(h->dev);
    pthread_mutex_unlock(&state->completion_mtx);
}

static int rbd_handler_init(struct rbd_handler *h, struct tcmu_device *dev)
{
    int i;

    h->dev = dev;
    h->stop = 0;

    pthread_mutex_init(&h->mtx, NULL);
    pthread_mutex_init(&h->io_mtx, NULL);
    pthread_cond_init(&h->io_cond, NULL);

    h->free_io_nr = IODEPTH;
    for (i = 0; i < IODEPTH; i++) {
        h->free_ios[i] = &h->ios[i];
        h->free_ios[i]->flags = IO_F_FREE;
    }

    return 0;
}

static void rbd_handler_destroy(struct rbd_handler *h)
{
    while (h->free_io_nr != IODEPTH) {

    }

    pthread_mutex_lock(&h->io_mtx);
    while (h->free_io_nr != IODEPTH) {
        pthread_cond_wait(&h->io_cond, &h->io_mtx);
    }
    pthread_mutex_unlock(&h->io_mtx);

    pthread_cond_destroy(&h->io_cond);
    pthread_mutex_destroy(&h->io_mtx);
    pthread_mutex_destroy(&h->mtx);
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

    r = rados_create(&rbd->cluster, NULL);
    if (r < 0) {
        errp("rados_create failed, code: %d\n", r);
        goto out;
    }

    r = rados_conf_read_file(rbd->cluster, NULL);
    if (r < 0) {
        errp("rados_conf_read_file failed, code: %d\n", r);
        goto out_shutdown;
    }

    r = rados_connect(rbd->cluster);
    if (r < 0) {
        errp("rados_connect failed, code: %d\n", r);
        goto out_shutdown;
    }

    r = rados_ioctx_create(rbd->cluster, opts->pool_name, &rbd->ioctx);
    if (r < 0) {
        errp("rados_ioctx_create failed, code: %d\n", r);
        goto out_shutdown;
    }

    r = rbd_open(rbd->ioctx, opts->image_name, &rbd->image, opts->snap_name);
    if (r < 0) {
        errp("rbd_open failed, code: %d\n", r);
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
    return r;
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
    r = rbd_connect(state);
    if (r) {
        errp("connect to rbd failed, code: %d\n", r);
        goto out;
    }

    pthread_mutex_init(&state->completion_mtx, NULL);

    r = rbd_handler_init(&state->h, dev);
    if (r) {
        errp("init rbd handler failed, code: %d\n", r);
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

    /* disconnect */
    rbd_disconnect(state);

    rbd_handler_destroy(&state->h);

    free(opts->cluster_name);
    free(opts->client_name);
    free(opts->pool_name);
    free(opts->image_name);
    free(opts->snap_name);

    pthread_mutex_destroy(&state->completion_mtx);

    free(state);
}

static struct io *rbd_get_io(struct rbd_handler *h)
{
    struct io *io = NULL;

    pthread_mutex_lock(&h->io_mtx);
    while (!h->free_io_nr) {
        pthread_mutex_lock(&h->mtx);
        if (h->stop) {
            pthread_mutex_unlock(&h->mtx);
            return NULL;
        }
        pthread_mutex_unlock(&h->mtx);

        pthread_cond_wait(&h->io_cond, &h->io_mtx);
    }
    io = h->free_ios[--h->free_io_nr];
    pthread_mutex_unlock(&h->io_mtx);

    io->flags = IO_F_PENDING;
    io->xfer_buf = NULL;

    return io;
}

static void rbd_put_io(struct rbd_handler *h, struct io *io)
{
    io->flags = IO_F_FREE;
    free(io->xfer_buf);

    pthread_mutex_lock(&h->io_mtx);
    h->free_ios[h->free_io_nr++] = io;
    pthread_cond_signal(&h->io_cond);
    pthread_mutex_unlock(&h->io_mtx);
}

static void rbd_io_callback(rbd_completion_t comp, void *data) {
    struct io *io = data;
    struct rbd_handler *h = io->h;
    struct tcmulib_cmd *cmd = io->cmd;
    uint8_t *cdb = cmd->cdb;
    struct iovec *iovec = cmd->iovec;
    size_t iov_cnt = cmd->iov_cnt;
    uint8_t *sense = cmd->sense_buf;
    struct rbd_state *state = tcmu_get_dev_private(h->dev);
    uint32_t block_size = state->block_size;
    uint32_t cmp_offset;
    uint8_t scsi_cmd = cdb[0];;
    ssize_t r;
    int result = SAM_STAT_GOOD;

    r = rbd_aio_get_return_value(io->completion);
    if (r < 0) {
        errp("async io error, code: %d\n", r);
        result = set_medium_error(sense);

        rbd_aio_release(io->completion);
        rbd_put_io(h, io);
        rbd_complete_cmd(h, cmd, result);
        return;
    }

    switch (io->data_op) {
    case IO_D_READ:
        switch (scsi_cmd) {
        case COMPARE_AND_WRITE:
            cmp_offset = tcmu_compare_with_iovec(io->xfer_buf, iovec,
                    io->xfer_buflen);
            if (cmp_offset != -1) {
                result = tcmu_set_sense_data(sense, MISCOMPARE,
                        ASC_MISCOMPARE_DURING_VERIFY_OPERATION,
                        &cmp_offset);
                break;
            }

            io->data_op = IO_D_WRITE;

            /* TODO: set the real iov_cnt */
            tcmu_seek_in_iovec(iovec, io->xfer_buflen);
            tcmu_memcpy_from_iovec(io->xfer_buf, io->xfer_buflen, iovec,
                    iov_cnt);

            result = rbd_do_io(io);
            break;
        case WRITE_VERIFY:
        case WRITE_VERIFY_12:
        case WRITE_VERIFY_16:
            cmp_offset = tcmu_compare_with_iovec(io->xfer_buf, iovec,
                    io->xfer_buflen);
            if (cmp_offset != -1) {
                result = tcmu_set_sense_data(sense, MISCOMPARE,
                        ASC_MISCOMPARE_DURING_VERIFY_OPERATION,
                        &cmp_offset);
            }
            break;
        default:
            tcmu_memcpy_into_iovec(iovec, iov_cnt, io->xfer_buf,
                    io->xfer_buflen);
            break;
        }
        break;
    case IO_D_WRITE:
        switch (scsi_cmd) {
        case COMPARE_AND_WRITE:
            /* If FUA or !WCE then sync */
            if (((scsi_cmd != WRITE_6) && (cdb[1] & 0x8)) || !state->wce) {
                io->data_op = IO_D_FLUSH;

                result = rbd_do_io(io);
            }
            break;
        case WRITE_VERIFY:
        case WRITE_VERIFY_12:
        case WRITE_VERIFY_16:
            /* verify */
            tcmu_memcpy_into_iovec(iovec, iov_cnt, io->xfer_buf,
                    io->xfer_buflen);

            io->data_op = IO_D_READ;

            result = rbd_do_io(io);
            break;
        case WRITE_SAME:
        case WRITE_SAME_16:
            io->offset += block_size;
            io->resid -= block_size;

            if (io->resid) {
                uint32_t val32;
                uint64_t val64;

                switch (cdb[1] & 0x06) {
                case 0x02: /* PBDATA==0 LBDATA==1 */
                    val32 = htobe32(io->offset);
                    memcpy(io->xfer_buf, &val32, 4);
                    break;
                case 0x04: /* PBDATA==1 LBDATA==0 */
                    /* physical sector format */
                    /* hey this is wrong val! But how to fix? */
                    val64 = htobe64(io->offset);
                    memcpy(io->xfer_buf, &val64, 8);
                    break;
                default:
                    /* FIXME */
                    errp("PBDATA and LBDATA set!!!\n");
                }

                io->data_op = IO_D_WRITE;

                result = rbd_do_io(io);
            }
            break;
        default:
            break;
        }
        break;
    case IO_D_DISCARD:
        switch (scsi_cmd) {
        case WRITE_SAME:
        case WRITE_SAME_16:
            if (cdb[1] & 0x08) {
                /* rbd_aio_discard always return 0 */
                break;
            }
            break;
        default:
            break;
        }
        break;
    default:
        break;
    }

    if (result == TCMU_ASYNC_HANDLED) {
        return;
    }

    rbd_aio_release(io->completion);
    rbd_put_io(h, io);
    rbd_complete_cmd(h, cmd, result);
}

/*
 * Return TCMU_ASYNC_HANDLED or scsi status
 */
static int rbd_do_io(struct io *io)
{
    struct rbd_handler *h = io->h;
    struct tcmulib_cmd *cmd = io->cmd;
    struct rbd_state *state = tcmu_get_dev_private(h->dev);
    struct rbd_data *rbd = &state->rbd;
    uint8_t *sense = cmd->sense_buf;
    int r = 0;
    int result = TCMU_ASYNC_HANDLED;

    r = rbd_aio_create_completion(io, rbd_io_callback, &io->completion);
    if (r < 0) {
        errp("rbd_aio_create_completion failed, code: %d\n", r);
        result = set_medium_error(sense);
        return result;
    }

    switch (io->data_op) {
    case IO_D_READ:
        r = rbd_aio_read(rbd->image, io->offset, io->xfer_buflen, io->xfer_buf,
                io->completion);
        if (r < 0) {
            errp("rbd_aio_read failed, code: %d\n", r);
            result = set_medium_error(sense);
        }
        break;
    case IO_D_WRITE:
        r = rbd_aio_write(rbd->image, io->offset, io->xfer_buflen, io->xfer_buf,
                io->completion);
        if (r < 0) {
            errp("rbd_aio_write failed, code: %d\n", r);
            result = set_medium_error(sense);
        }
        break;
    case IO_D_FLUSH:
        r = rbd_aio_flush(rbd->image, io->completion);
        if (r < 0) {
            errp("rbd_aio_flush failed, code: %d\n", r);
            result = set_medium_error(sense);
        }
        break;
    case IO_D_DISCARD:
        r = rbd_aio_discard(rbd->image, io->offset, io->xfer_buflen,
                io->completion);
        if (r < 0) {
            errp("rbd_aio_discard failed, code: %d\n", r);
            result = set_medium_error(sense);
        }
        break;
    default:
        errp("Warning: unhandled data_op: %d\n", io->data_op);
        result = set_medium_error(sense);
        break;
    }

    if (result == TCMU_ASYNC_HANDLED) {
        io->flags = IO_F_INFLIGHT;
    } else {
        rbd_aio_release(io->completion);
    }

    return result;
}

static int rbd_handle_cmd(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
    struct rbd_state *state = tcmu_get_dev_private(dev);
    struct rbd_handler *h = &state->h;
    uint8_t *cdb = cmd->cdb;
    struct iovec *iovec = cmd->iovec;
    size_t iov_cnt = cmd->iov_cnt;
    uint8_t *sense = cmd->sense_buf;
    uint32_t block_size = state->block_size;
    uint64_t offset = block_size * tcmu_get_lba(cdb);
    uint32_t length = block_size * tcmu_get_xfer_length(cdb);
    struct io *io = NULL;
    void *buf = NULL;
    uint8_t scsi_cmd = cdb[0];
    uint32_t val32;
    uint64_t val64;
    int result = TCMU_ASYNC_HANDLED;

    switch (scsi_cmd) {
    case INQUIRY:
        result = tcmu_emulate_inquiry(dev, cdb, iovec, iov_cnt, sense);
        break;
    case TEST_UNIT_READY:
        result = tcmu_emulate_test_unit_ready(cdb, iovec, iov_cnt, sense);
        break;
    case SERVICE_ACTION_IN_16:
        if (cdb[1] == READ_CAPACITY_16)
            result = tcmu_emulate_read_capacity_16(state->num_lbas,
                    state->block_size, cdb, iovec, iov_cnt, sense);
        break;
    case MODE_SENSE:
    case MODE_SENSE_10:
        result = tcmu_emulate_mode_sense(cdb, iovec, iov_cnt, sense);
        break;
    case MODE_SELECT:
    case MODE_SELECT_10:
        result = tcmu_emulate_mode_select(cdb, iovec, iov_cnt, sense);
        break;
    case COMPARE_AND_WRITE:
        /* Blocks are transferred twice, first the set that
         * we compare to the existing data, and second the set
         * to write if the compare was successful.
         */
        length >>= 1;
        buf = malloc(length);
        if (!buf) {
            result = tcmu_set_sense_data(sense, HARDWARE_ERROR,
                    ASC_INTERNAL_TARGET_FAILURE, NULL);
            break;
        }

        io = rbd_get_io(h);
        if (!io) {
            free(buf);
            result = TCMU_NOT_HANDLED;
            break;
        }
        io->data_op = IO_D_READ;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = length;
        break;
    case SYNCHRONIZE_CACHE:
    case SYNCHRONIZE_CACHE_16:
        if (cdb[1] & 0x2) {
            result = tcmu_set_sense_data(sense, ILLEGAL_REQUEST,
                    ASC_INVALID_FIELD_IN_CDB, NULL);
        } else {
            io = rbd_get_io(h);
            if (!io) {
                result = TCMU_NOT_HANDLED;
                break;
            }
            io->data_op = IO_D_FLUSH;
        }
        break;
    case WRITE_VERIFY:
    case WRITE_VERIFY_12:
    case WRITE_VERIFY_16:
    case WRITE_6:
    case WRITE_10:
    case WRITE_12:
    case WRITE_16:
        buf = malloc(length);
        if (!buf) {
            result = tcmu_set_sense_data(sense, HARDWARE_ERROR,
                    ASC_INTERNAL_TARGET_FAILURE, NULL);
            break;
        }

        io = rbd_get_io(h);
        if (!io) {
            free(buf);
            result = TCMU_NOT_HANDLED;
            break;
        }
        tcmu_memcpy_from_iovec(buf, length, iovec, iov_cnt);
        io->data_op = IO_D_WRITE;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = length;
        break;
    case WRITE_SAME:
    case WRITE_SAME_16:
        if (!state->tpws) {
            result = tcmu_set_sense_data(sense, ILLEGAL_REQUEST,
                    ASC_INVALID_FIELD_IN_CDB, NULL);
            break;
        }

        /* WRITE_SAME used to punch hole in file */
        if (cdb[1] & 0x08) {
            io = rbd_get_io(h);
            if (!io) {
                result = TCMU_NOT_HANDLED;
                break;
            }
            io->data_op = IO_D_DISCARD;
            io->offset = offset;
            io->xfer_buflen = length;
            break;
        }

        buf = malloc(length);
        if (!buf) {
            result = tcmu_set_sense_data(sense, HARDWARE_ERROR,
                    ASC_INTERNAL_TARGET_FAILURE, NULL);
            break;
        }

        switch (cdb[1] & 0x06) {
        case 0x02: /* PBDATA==0 LBDATA==1 */
            val32 = htobe32(offset);
            memcpy(buf, &val32, 4);
            break;
        case 0x04: /* PBDATA==1 LBDATA==0 */
            /* physical sector format */
            /* hey this is wrong val! But how to fix? */
            val64 = htobe64(offset);
            memcpy(buf, &val64, 8);
            break;
        default:
            /* FIXME */
            errp("PBDATA and LBDATA set!!!\n");
        }

        io = rbd_get_io(h);
        if (!io) {
            free(buf);
            result = TCMU_NOT_HANDLED;
            break;
        }
        tcmu_memcpy_from_iovec(buf, length, iovec, iov_cnt);
        io->data_op = IO_D_WRITE;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = block_size;
        io->resid = length;
        break;
    case READ_6:
    case READ_10:
    case READ_12:
    case READ_16:
        buf = malloc(length);
        if (!buf) {
            result = tcmu_set_sense_data(sense, HARDWARE_ERROR,
                    ASC_INTERNAL_TARGET_FAILURE, NULL);
            break;
        }

        io = rbd_get_io(h);
        if (!io) {
            free(buf);
            result = TCMU_NOT_HANDLED;
            break;
        }
        io->data_op = IO_D_READ;
        io->offset = offset;
        io->xfer_buf = buf;
        io->xfer_buflen = length;
        break;
    case UNMAP:
        if (!state->tpu) {
            result = tcmu_set_sense_data(sense, ILLEGAL_REQUEST,
                    ASC_INVALID_FIELD_IN_CDB, NULL);
            break;
        }

        /* TODO: implement UNMAP */
        result = tcmu_set_sense_data(sense, ILLEGAL_REQUEST,
                ASC_INVALID_FIELD_IN_CDB, NULL);
        break;
    default:
        result = TCMU_NOT_HANDLED;
        break;
    }

    if (result == TCMU_ASYNC_HANDLED) {
        io->h = h;
        io->cmd = cmd;

        result = rbd_do_io(io);
        if (result != TCMU_ASYNC_HANDLED) {
            /* queue aio failed */
            rbd_put_io(h, io);
        }
    }

    return result;
}

static const char rbd_cfg_desc[] =
        "The rbd image to use as a backstore.";

static struct tcmur_handler rbd_handler = {
    .cfg_desc = rbd_cfg_desc,
    .check_config = rbd_check_config,
    .open = rbd_dev_open,
    .close = rbd_dev_close,
    .name = "RBD-backed Handler",
    .subtype = "rbd",
    .handle_cmd = rbd_handle_cmd,
};

/* Entry point must be named "handler_init". */
void handler_init(void)
{
    tcmur_register_handler(&rbd_handler);
}
