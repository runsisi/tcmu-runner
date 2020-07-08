/*
 * Copyright 2016, China Mobile, Inc.
 *
 * This file is licensed to you under your choice of the GNU Lesser
 * General Public License, version 2.1 or any later version (LGPLv2.1 or
 * later), or the Apache License 2.0.
 */

#define _GNU_SOURCE
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <fcntl.h>
#include <endian.h>
#include <errno.h>
#include <pthread.h>

#include <scsi/scsi.h>

#include "tcmu-runner.h"
#include "tcmur_cmd_handler.h"
#include "libtcmu.h"
#include "tcmur_device.h"

#include <rbd/librbd.h>
#include <rados/librados.h>

#include <sys/epoll.h>
#include <sys/eventfd.h>

/*
 * rbd_lock_acquire exclusive lock support was added in librbd 0.1.11
 */
#if LIBRBD_VERSION_CODE >= LIBRBD_VERSION(0, 1, 11)
#define RBD_LOCK_ACQUIRE_SUPPORT
#endif

/* rbd_aio_discard added in 0.1.2 */
#if LIBRBD_VERSION_CODE >= LIBRBD_VERSION(0, 1, 2)
#define RBD_DISCARD_SUPPORT
#endif

/*
 * rbd_aio_writesame support was added in librbd 1.12.0
 */
#if LIBRBD_VERSION_CODE >= LIBRBD_VERSION(1, 12, 0) || LIBRBD_SUPPORTS_WRITESAME
#define RBD_WRITE_SAME_SUPPORT
#endif

/* defined in librbd.h if supported */
#ifdef LIBRBD_SUPPORTS_IOVEC
#if LIBRBD_SUPPORTS_IOVEC
#define RBD_IOVEC_SUPPORT
#endif
#endif

/* defined in librbd.h if supported */
#ifdef LIBRBD_SUPPORTS_COMPARE_AND_WRITE
#if LIBRBD_SUPPORTS_COMPARE_AND_WRITE
#define RBD_COMPARE_AND_WRITE_SUPPORT
#endif
#endif

#define TCMU_RBD_LOCKER_TAG_KEY "tcmu_rbd_locker_tag"
#define TCMU_RBD_LOCKER_TAG_FMT "tcmu_tag=%hu,rbd_client=%s"
#define TCMU_RBD_LOCKER_BUF_LEN 256

struct tcmu_rbd_state {
	rados_t cluster;
	rados_ioctx_t io_ctx;
	rbd_image_t image;

	int fd; /* poll aio completion */

	char *image_name;
	char *pool_name;
	char *osd_op_timeout;
	char *conf_path;
	char *id;
};

enum rbd_aio_type {
	RBD_AIO_TYPE_WRITE = 0,
	RBD_AIO_TYPE_READ,
	RBD_AIO_TYPE_CAW
};

struct rbd_aio_cb {
	struct tcmu_device *dev;
	struct tcmur_cmd *tcmur_cmd;

	enum rbd_aio_type type;
	union {
		struct {
			int64_t length;
		} read;
		struct {
			uint64_t offset;
			uint64_t miscompare_offset;
		} caw;
	};
	char *bounce_buffer;
	struct iovec *iov;
	size_t iov_cnt;
};

#ifndef EPOLLEXCLUSIVE
/*
 * linux/include/uapi/linux/eventpoll.h
 * https://bugzilla.redhat.com/show_bug.cgi?id=1426133
 */
#define EPOLLEXCLUSIVE (1u << 28)
#endif

#define TCMU_RBD_NR_AIO_THREADS     2   /* polling threads for aio completion */
#define TCMU_RBD_MAX_REVENTS        128 /* rbd device nr. */
#define TCMU_RBD_AIO_QUEUE_DEPTH    512 /* rbd device io depth */

static int tcmu_rbd_nr_aio_threads = TCMU_RBD_NR_AIO_THREADS;
static int tcmu_rbd_max_revents = TCMU_RBD_MAX_REVENTS;
static int tcmu_rbd_aio_queue_depth = TCMU_RBD_AIO_QUEUE_DEPTH;

static int tcmu_rbd_aio_epfd = -1;
static pthread_t *tcmu_rbd_aio_threads = NULL;

/*
 * https://github.com/cirosantilli/cpp-cheat/blob/master/c/str2num.h
 */
int str2int(int *out, char *s, int base) {
    char *end;
    long l;
    if (s[0] == '\0' || isspace(s[0]))
        return -1;
    errno = 0;
    l = strtol(s, &end, base);
    /* Both checks are needed because INT_MAX == LONG_MAX is possible. */
    if (l > INT_MAX || (errno == ERANGE && l == LONG_MAX))
        return -1;
    if (l < INT_MIN || (errno == ERANGE && l == LONG_MIN))
        return -1;
    if (*end != '\0')
        return -1;
    *out = l;
    return 0;
}

static void rbd_finish_aio_generic(rbd_completion_t completion,
        struct rbd_aio_cb *aio_cb);

static int tcmu_rbd_handler_init();
static void *tcmu_rbd_aio_poller(void *arg);
static int tcmu_rbd_setup_aio_poll(struct tcmu_device *dev, int epfd);
static void tcmu_rbd_teardown_aio_poll(struct tcmu_device *dev, int epfd);

static int tcmu_rbd_handler_init()
{
    int ret;
    char *env_nr_threads, *env_max_revents, *env_q_depth;
    int nr_threads, max_revents, q_depth;
    int i;

    env_nr_threads = getenv("TCMU_RBD_NR_AIO_THREADS");
    env_max_revents = getenv("TCMU_RBD_MAX_REVENTS");
    env_q_depth = getenv("TCMU_RBD_AIO_QUEUE_DEPTH");

    if (env_nr_threads) {
        ret = str2int(&nr_threads, env_nr_threads, 10);
        if (ret < 0 || nr_threads < 1 || nr_threads > 100) {
            ret = -EINVAL;
            tcmu_err("invalid env setting for TCMU_RBD_NR_AIO_THREADS=%s", env_nr_threads);
            goto out;
        }
        tcmu_rbd_nr_aio_threads = nr_threads;
    }
    if (env_max_revents) {
        ret = str2int(&max_revents, env_max_revents, 10);
        if (ret < 0 || max_revents < 1 || max_revents > 1000) {
            ret = -EINVAL;
            tcmu_err("invalid env setting for TCMU_RBD_MAX_REVENTS=%s", env_max_revents);
            goto out;
        }
        tcmu_rbd_max_revents = max_revents;
    }
    if (env_q_depth) {
        ret = str2int(&q_depth, env_q_depth, 10);
        if (ret < 0 || q_depth < 1 || q_depth > 1000) {
            ret = -EINVAL;
            tcmu_err("invalid env setting for TCMU_RBD_AIO_QUEUE_DEPTH=%s", env_q_depth);
            goto out;
        }
        tcmu_rbd_aio_queue_depth = q_depth;
    }

    tcmu_rbd_aio_epfd = epoll_create1(EPOLL_CLOEXEC);
    if (tcmu_rbd_aio_epfd < 0) {
        ret = errno;
        tcmu_err("epoll_create1 failed. Err %d.\n", ret);
        goto out;
    }

    tcmu_rbd_aio_threads = calloc(tcmu_rbd_nr_aio_threads, sizeof(pthread_t));
    if (!tcmu_rbd_aio_threads) {
        ret = ENOMEM;
        tcmu_err("alloc mem for thread ptrs failed.\n");
        goto cleanup_epfd;
    }

    for (i = 0; i < tcmu_rbd_nr_aio_threads; i++) {
        ret = pthread_create(&tcmu_rbd_aio_threads[i], NULL,
                tcmu_rbd_aio_poller, &tcmu_rbd_aio_epfd);
        if (ret != 0) {
            tcmu_err("pthread_create failed. Err %d.\n", ret);
            goto cleanup_threads;
        }
    }

    return 0;

cleanup_threads:
    for (i = 0; i < tcmu_rbd_nr_aio_threads; i++) {
        if (tcmu_rbd_aio_threads[i]) {
            tcmu_thread_cancel(tcmu_rbd_aio_threads[i]);
        }
    }
    free(tcmu_rbd_aio_threads);
cleanup_epfd:
    close(tcmu_rbd_aio_epfd);
    tcmu_rbd_aio_epfd = -1;
out:
    return -ret;
}

static void *tcmu_rbd_aio_poller(void *arg)
{
    int epfd = *(int *)arg;
    int ret;
    int i, j;
    struct tcmu_device *dev;
    struct tcmu_rbd_state *state;
    /* ulimit -S -s, thread stack space should be enough */
    struct epoll_event revents[tcmu_rbd_max_revents];
    rbd_completion_t comps[tcmu_rbd_aio_queue_depth];
    int aio_nr;
    struct rbd_aio_cb *aio_cb;
    uint64_t counter;

    tcmu_set_thread_name("poller", NULL);

    while (1) {
        ret = epoll_wait(epfd, revents, tcmu_rbd_max_revents, -1);
        if (ret <= 0) {
            continue;
        }

        for (i = 0; i < ret; i++) {
            if (!(revents[i].events & EPOLLIN)) {
                continue;
            }

            dev = revents[i].data.ptr;
            state = tcmur_dev_get_private(dev);

            aio_nr = rbd_poll_io_events(state->image, comps, tcmu_rbd_aio_queue_depth);
            for (j = 0; j < aio_nr; j++) {
                aio_cb = rbd_aio_get_arg(comps[j]);

                /* decrement the semaphore */
                ret = read(state->fd, &counter, sizeof(counter));
                if (ret <= 0) {
                    ret = -errno;
                    tcmu_dev_warn(dev, "decrement semaphore failed. Err %d.\n", ret);
                }

                rbd_finish_aio_generic(comps[j], aio_cb);
            }
        }
    }

    return NULL;
}

static int tcmu_rbd_setup_aio_poll(struct tcmu_device *dev, int epfd)
{
    struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
    int ret;
    struct epoll_event ev;

    state->fd = eventfd(0, EFD_SEMAPHORE);
    if (state->fd < 0) {
        ret = -errno;
        tcmu_dev_err(dev, "create eventfd failed. Err %d.\n", ret);
        goto out;
    }

    ret = rbd_set_image_notification(state->image, state->fd, EVENT_TYPE_EVENTFD);
    if (ret < 0) {
        tcmu_dev_err(dev, "rbd_set_image_notification failed. Err %d.\n", ret);
        goto cleanup_fd;
    }

    /* EPOLLEXCLUSIVE requires kernel 4.5+
     * CentOS/RHEL has it backported since 3.10.0-514 */
    ev.events = EPOLLIN | EPOLLEXCLUSIVE;
    ev.data.ptr = (void *)dev;
    ret = epoll_ctl(epfd, EPOLL_CTL_ADD, state->fd, &ev);
    if (ret < 0) {
        ret = -errno;
        tcmu_dev_err(dev, "epoll_ctl failed. Err %d.\n", ret);
        goto cleanup_fd;
    }

    return 0;

cleanup_fd:
    close(state->fd);
    state->fd = -1;
out:
    return ret;
}

static void tcmu_rbd_teardown_aio_poll(struct tcmu_device *dev, int epfd)
{
    struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
    int ret;

    ret = epoll_ctl(epfd, EPOLL_CTL_DEL, state->fd, NULL);
    if (ret < 0) {
        ret = -errno;
        tcmu_dev_warn(dev, "epoll_ctl del %d from %d failed. Err %d.\n",
                state->fd, epfd, ret);
    }

    close(state->fd);

    state->fd = -1;
}

#ifdef LIBRADOS_SUPPORTS_SERVICES

#ifdef RBD_LOCK_ACQUIRE_SUPPORT
static void tcmu_rbd_service_status_update(struct tcmu_device *dev,
					   bool has_lock)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	char *status_buf = NULL;
	int ret;

	ret = asprintf(&status_buf, "%s%c%s%c", "lock_owner", '\0',
		       has_lock ? "true" : "false", '\0');
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not allocate status buf. Service will not be updated.\n");
		return;
	}

	ret = rados_service_update_status(state->cluster, status_buf);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not update service status. (Err %d)\n",
			     ret);
	}

	free(status_buf);
}
#endif /* RBD_LOCK_ACQUIRE_SUPPORT */

static int tcmu_rbd_service_register(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	struct utsname u;
	char *daemon_buf = NULL;
	char *metadata_buf = NULL;
	char *image_id_buf = NULL;
	int ret;

	ret = uname(&u);
	if (ret < 0) {
		ret = -errno;
		tcmu_dev_err(dev, "Could not query uname. (Err %d)\n", ret);
		return ret;
	}

	image_id_buf = malloc(RBD_MAX_BLOCK_NAME_SIZE);
	if (image_id_buf == NULL) {
		tcmu_dev_err(dev, "Could not allocate image id buf.\n");
		return -ENOMEM;
	}

	ret = rbd_get_id(state->image, image_id_buf, RBD_MAX_BLOCK_NAME_SIZE);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not retrieve image id.\n");
		goto free_image_id_buf;
	}

	ret = asprintf(&daemon_buf, "%s:%s/%s",
		       u.nodename, state->pool_name, state->image_name);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not allocate daemon buf.\n");
		ret = -ENOMEM;
		goto free_image_id_buf;
	}

	ret = asprintf(&metadata_buf, "%s%c%s%c%s%c%s%c%s%c%s%c",
		       "pool_name", '\0', state->pool_name, '\0',
		       "image_name", '\0', state->image_name, '\0',
		       "image_id", '\0', image_id_buf, '\0');
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not allocate metadata buf.\n");
		ret = -ENOMEM;
		goto free_daemon_buf;
	}

	ret = rados_service_register(state->cluster, "tcmu-runner",
				     daemon_buf, metadata_buf);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not register service to cluster. (Err %d)\n",
			     ret);
	}

	free(metadata_buf);
free_daemon_buf:
	free(daemon_buf);
free_image_id_buf:
	free(image_id_buf);
	return ret;
}

#else /* LIBRADOS_SUPPORTS_SERVICES */

static int tcmu_rbd_service_register(struct tcmu_device *dev)
{
	/* Ignorable. Just log in dbg mode just in case. */
	tcmu_dev_dbg(dev, "Ceph service registration not supported.\n");
	return 0;
}

#ifdef RBD_LOCK_ACQUIRE_SUPPORT
static void tcmu_rbd_service_status_update(struct tcmu_device *dev,
					   bool has_lock)
{
}
#endif /* RBD_LOCK_ACQUIRE_SUPPORT */

#endif /* LIBRADOS_SUPPORTS_SERVICES */

static char *tcmu_rbd_find_quote(char *string)
{
	/* ignore escaped quotes */
	while (true) {
		string = strpbrk(string, "\"\\");
		if (!string) {
			break;
		}

		if (*string == '"') {
			break;
		}

		if (*++string == '\0') {
			break;
		}

		/* skip past the escaped character */
		++string;
	}
	return string;
}

static bool tcmu_rbd_match_device_class(struct tcmu_device *dev,
					const char *crush_rule,
					const char *device_class)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	char *mon_cmd_bufs[2] = {NULL, NULL};
	char *mon_buf = NULL, *mon_status_buf = NULL;
	size_t mon_buf_len = 0, mon_status_buf_len = 0;
	int ret;
	bool match = false;

	/* request a list of crush rules associated to the device class */
	ret = asprintf(&mon_cmd_bufs[0],
		       "{\"prefix\": \"osd crush rule ls-by-class\", "
		        "\"class\": \"%s\", \"format\": \"json\"}",
		       device_class);
	if (ret < 0) {
		tcmu_dev_warn(dev, "Could not allocate crush rule ls-by-class command.\n");
		return false;
	}

	ret = rados_mon_command(state->cluster, (const char **)mon_cmd_bufs, 1,
				"", 0, &mon_buf, &mon_buf_len,
				&mon_status_buf, &mon_status_buf_len);
	free(mon_cmd_bufs[0]);
	if (ret == -ENOENT) {
		tcmu_dev_dbg(dev, "%s not a registered device class.\n", device_class);
		return false;
	} else if (ret < 0 || !mon_buf) {
		tcmu_dev_warn(dev, "Could not retrieve pool crush rule ls-by-class (Err %d)\n",
			      ret);
		return false;
	}
	rados_buffer_free(mon_status_buf);

	/* expected JSON output: ["<rule name>",["<rule name>"...]] */
	mon_buf[mon_buf_len - 1] = '\0';
	match = (strstr(mon_buf, crush_rule) != NULL);
	rados_buffer_free(mon_buf);
	return match;
}

static void tcmu_rbd_detect_device_class(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	char *mon_cmd_bufs[2] = {NULL, NULL};
	char *mon_buf = NULL, *mon_status_buf = NULL;
	size_t mon_buf_len = 0, mon_status_buf_len = 0;
	char *crush_rule = NULL, *crush_rule_end = NULL;
	int ret;

	/* request the crush rule name for the image's pool */
	ret = asprintf(&mon_cmd_bufs[0],
		       "{\"prefix\": \"osd pool get\", "
		        "\"pool\": \"%s\", "
		        "\"var\": \"crush_rule\", "
			"\"format\": \"json\"}", state->pool_name);
	if (ret < 0) {
		tcmu_dev_warn(dev, "Could not allocate crush rule command.\n");
		return;
	}

	ret = rados_mon_command(state->cluster, (const char **)mon_cmd_bufs, 1,
				"", 0, &mon_buf, &mon_buf_len,
				&mon_status_buf, &mon_status_buf_len);
	free(mon_cmd_bufs[0]);
	if (ret < 0 || !mon_buf) {
		tcmu_dev_warn(dev, "Could not retrieve pool crush rule (Err %d)\n",
			      ret);
		return;
	}
	rados_buffer_free(mon_status_buf);

	/* expected JSON output: "{..."crush_rule":"<rule name>"...}" */
	mon_buf[mon_buf_len - 1] = '\0';
	crush_rule = strstr(mon_buf, "\"crush_rule\":\"");
	if (!crush_rule) {
		tcmu_dev_warn(dev, "Could not locate crush rule key\n");
		rados_buffer_free(mon_buf);
		return;
	}

	/* skip past key to the start of the quoted rule name */
	crush_rule += 13;
	crush_rule_end = tcmu_rbd_find_quote(crush_rule + 1);
	if (!crush_rule_end) {
		tcmu_dev_warn(dev, "Could not extract crush rule\n");
		rados_buffer_free(mon_buf);
		return;
	}

	*(crush_rule_end + 1) = '\0';
	crush_rule = strdup(crush_rule);
	rados_buffer_free(mon_buf);
	tcmu_dev_dbg(dev, "Pool %s using crush rule %s\n", state->pool_name,
		     crush_rule);

	if (tcmu_rbd_match_device_class(dev, crush_rule, "ssd") ||
	    tcmu_rbd_match_device_class(dev, crush_rule, "nvme")) {
		tcmu_dev_dbg(dev, "Pool %s associated to solid state device class.\n",
			     state->pool_name);
		tcmu_dev_set_solid_state_media(dev, true);
	}

	free(crush_rule);
}

static void tcmu_rbd_image_close(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);

	rbd_close(state->image);
	rados_ioctx_destroy(state->io_ctx);
	rados_shutdown(state->cluster);

	state->cluster = NULL;
	state->io_ctx = NULL;
	state->image = NULL;
}

static int timer_check_and_set_def(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	char buf[128];
	int grace, interval, ret, len;
	float timeout;

	ret = rados_conf_get(state->cluster, "osd_heartbeat_grace",
			     buf, 128);
	if (ret) {
		tcmu_dev_err(dev, "Failed to get cluster's default osd_heartbeat_grace\n");
		return ret;
	}
	grace = atoi(buf);

	ret = rados_conf_get(state->cluster, "osd_heartbeat_interval",
			     buf, 128);
	if (ret) {
		tcmu_dev_err(dev, "Failed to get cluster's default osd_heartbeat_interval\n");
		return ret;
	}
	interval = atoi(buf);

	ret = rados_conf_get(state->cluster, "rados_osd_op_timeout",
			     buf, 128);
	if (ret) {
		tcmu_dev_err(dev, "Failed to get cluster's default rados_osd_op_timeout\n");
		return ret;
	}
	timeout = atof(buf);

	tcmu_dev_dbg(dev, "The cluster's default osd op timeout(%f), osd heartbeat grace(%d) interval(%d)\n",
		     timeout, grace, interval);

	/* Frist: Try to use new osd op timeout value */
	if (state->osd_op_timeout && atof(state->osd_op_timeout) > grace + interval)
		goto set;

	/* Second: Try to use the default osd op timeout value as read from the cluster */
	if (timeout > grace + interval) {
		tcmu_dev_dbg(dev, "The osd op timeout will remain the default value: %f\n", timeout);
		return 0;
	}

	tcmu_dev_warn(dev, "osd op timeout (%s) must be larger than osd heartbeat grace (%d) + interval (%d)!\n",
		      state->osd_op_timeout, grace, interval);

	/*
	 * At last: Set the default rados_osd_op_timeout to grace + interval + 5
	 * to make sure rados_osd_op_timeout > grace + interval.
	 */
	len = sprintf(buf, "%d", grace + interval + 5);
	buf[len] = '\0';

	if (state->osd_op_timeout)
		free(state->osd_op_timeout);

	state->osd_op_timeout = strdup(buf);
	if (!state->osd_op_timeout) {
		tcmu_dev_err(dev, "Failed to alloc memory for ->osd_op_timeout\n");
		return -ENOMEM;
	}

	tcmu_dev_warn(dev, "Will set the osd op timeout to %s instead!\n",
		      state->osd_op_timeout);

set:
	return rados_conf_set(state->cluster, "rados_osd_op_timeout",
			      state->osd_op_timeout);
}

static int tcmu_rbd_image_open(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret;

	ret = rados_create(&state->cluster, state->id);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not create cluster. (Err %d)\n", ret);
		return ret;
	}

	/* Try default location when conf_path=NULL, but ignore failure */
	ret = rados_conf_read_file(state->cluster, state->conf_path);
	if (state->conf_path && ret < 0) {
		tcmu_dev_err(dev, "Could not read config %s (Err %d)",
			     state->conf_path, ret);
		goto rados_shutdown;
	}

	rados_conf_set(state->cluster, "rbd_cache", "false");

	ret = timer_check_and_set_def(dev);
	if (ret)
		tcmu_dev_warn(dev,
			      "Could not set rados osd op timeout to %s (Err %d. Failover may be delayed.)\n",
			      state->osd_op_timeout, ret);

	ret = rados_connect(state->cluster);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not connect to cluster. (Err %d)\n",
			     ret);
		goto rados_shutdown;
	}

	tcmu_rbd_detect_device_class(dev);
	ret = rados_ioctx_create(state->cluster, state->pool_name,
				 &state->io_ctx);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not create ioctx for pool %s. (Err %d)\n",
			     state->pool_name, ret);
		goto rados_shutdown;
	}

	ret = rbd_open(state->io_ctx, state->image_name, &state->image, NULL);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not open image %s. (Err %d)\n",
			     state->image_name, ret);
		goto rados_destroy;
	}

	ret = tcmu_rbd_service_register(dev);
	if (ret < 0)
		goto rbd_close;

	return 0;

rbd_close:
	rbd_close(state->image);
	state->image = NULL;
rados_destroy:
	rados_ioctx_destroy(state->io_ctx);
	state->io_ctx = NULL;
rados_shutdown:
	rados_shutdown(state->cluster);
	state->cluster = NULL;
	return ret;
}

#ifdef RBD_LOCK_ACQUIRE_SUPPORT

/*
 * Returns:
 * 0 = client is not owner.
 * 1 = client is owner.
 * -ESHUTDOWN/-EBLACKLISTED(-108) = client is blacklisted.
 * -ETIMEDOUT = rados osd op timeout has expired.
 * -EIO = misc error.
 */
static int tcmu_rbd_has_lock(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret, is_owner;

	ret = rbd_is_exclusive_lock_owner(state->image, &is_owner);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not check lock ownership. Error: %s.\n",
			     strerror(-ret));
		if (ret == -ESHUTDOWN || ret == -ETIMEDOUT)
			return ret;

		/* let initiator figure things out */
		return -EIO;
	} else if (is_owner) {
		tcmu_dev_dbg(dev, "Is owner\n");
		return 1;
	}
	tcmu_dev_dbg(dev, "Not owner\n");

	return 0;
}

static int tcmu_rbd_get_lock_state(struct tcmu_device *dev)
{
	int ret;

	ret = tcmu_rbd_has_lock(dev);
	if (ret == 1)
		return TCMUR_DEV_LOCK_LOCKED;
	else if (ret == 0 || ret == -ESHUTDOWN)
		return TCMUR_DEV_LOCK_UNLOCKED;
	else
		return TCMUR_DEV_LOCK_UNKNOWN;
}

/**
 * tcmu_rbd_lock_break - break rbd exclusive lock if needed
 * @dev: device to break the lock for.
 *
 * Returns:
 * 0 = lock has been broken.
 * -ETIMEDOUT = could not complete operation in rados osd op timeout seconds.
 * -Ezyx = misc failure.
 */
static int tcmu_rbd_lock_break(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	rbd_lock_mode_t lock_mode;
	char *owners[1];
	size_t num_owners = 1;
	int ret;

	ret = rbd_lock_get_owners(state->image, &lock_mode, owners,
				  &num_owners);
	if (ret == -ENOENT || (!ret && !num_owners))
		return 0;

	if (ret < 0) {
		tcmu_dev_err(dev, "Could not get lock owners to break lock %d\n",
			     ret);
		return ret;
	}

	if (lock_mode != RBD_LOCK_MODE_EXCLUSIVE) {
		tcmu_dev_err(dev, "Invalid lock type (%d) found\n", lock_mode);
		ret = -EIO;
		goto free_owners;
	}

	tcmu_dev_dbg(dev, "Attempting to break lock from %s.\n", owners[0]);

	ret = rbd_lock_break(state->image, lock_mode, owners[0]);
	if (ret < 0)
		tcmu_dev_err(dev, "Could not break lock from %s. (Err %d)\n",
			     owners[0], ret);
free_owners:
	rbd_lock_get_owners_cleanup(owners, num_owners);
	return ret;
}

static int tcmu_rbd_to_sts(int rc)
{
	switch (rc) {
	case 0:
		return TCMU_STS_OK;
	case -ESHUTDOWN:
		return TCMU_STS_FENCED;
	case -ENOENT:
		return TCMU_STS_NO_LOCK_HOLDERS;
	case -ETIMEDOUT:
		return TCMU_STS_TIMEOUT;
	case -ENOMEM:
		return TCMU_STS_NO_RESOURCE;
	default:
		return TCMU_STS_HW_ERR;
	}
}

static int tcmu_rbd_get_lock_tag(struct tcmu_device *dev, uint16_t *tag)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	char *metadata_owner, *owners[1];
	size_t num_owners = 1;
	rbd_lock_mode_t lock_mode;
	char buf[TCMU_RBD_LOCKER_BUF_LEN];
	size_t buf_len = TCMU_RBD_LOCKER_BUF_LEN;
	int ret;

	memset(buf, 0, buf_len);

	ret = rbd_metadata_get(state->image, TCMU_RBD_LOCKER_TAG_KEY,
			      buf, &buf_len);
	tcmu_dev_dbg(dev, "get meta got %d %s\n", ret, buf);
	if (ret)
		goto done;

	ret = rbd_lock_get_owners(state->image, &lock_mode, owners,
				  &num_owners);
	tcmu_dev_dbg(dev, "get lockowner got %d\n", ret);
	if (ret)
		goto done;
	if (!num_owners) {
		/* there would be stale metadata due to a crash */
		ret = -ENOENT;
		goto done;
	}

	metadata_owner = strstr(buf, "rbd_client=");
	if (!metadata_owner) {
		tcmu_dev_err(dev, "Invalid lock tag %s.\n", buf);
		/* Force initiator to retry STPG */
		ret = -ENOENT;
		goto free_owners;
	}

	metadata_owner += 11;
	if (strcmp(metadata_owner, owners[0])) {
		tcmu_dev_dbg(dev, "owner mismatch %s %s\n", metadata_owner,
			     owners[0]);
		/*
		 * A node could be in the middle of grabbing the lock or it
		 * failed midway. Force tcmu to report all standby so the
		 * initiator retries the STPG for the failure case.
		 */
		ret = -ENOENT;
		goto free_owners;
	}

	ret = sscanf(buf, "tcmu_tag=%hu,%*s", tag);
	if (ret != 1) {
		tcmu_dev_err(dev, "Invalid lock tag %s.\n", buf);
		/* Force initiator to retry STPG */
		ret = -ENOENT;
		goto free_owners;
	}
	ret = 0;

free_owners:
	if (num_owners)
		rbd_lock_get_owners_cleanup(owners, num_owners);

done:
	return tcmu_rbd_to_sts(ret);
}

static int tcmu_rbd_set_lock_tag(struct tcmu_device *dev, uint16_t tcmu_tag)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	rbd_lock_mode_t lock_mode;
	char *owners[1];
	size_t num_owners = 1;
	char *tcmu_rbd_tag;
	int ret;

	/*
	 * We cannot take the lock and set the tag atomically. In case
	 * we fail here and are in an inconsistent state, we attach the rbd
	 * client lock info along with the tcmu locker tag so remote nodes
	 * can check the rbd info against rbd_lock_get_owners to determine if
	 * the tcmu locker tag is current/valid.
	 */
	ret = rbd_lock_get_owners(state->image, &lock_mode, owners,
				  &num_owners);
	tcmu_dev_dbg(dev, "set tag get lockowner got %d %zd\n", ret, num_owners);
	if (ret)
		return ret;

	if (!num_owners)
		return -ENOENT;

	ret = asprintf(&tcmu_rbd_tag, TCMU_RBD_LOCKER_TAG_FMT, tcmu_tag,
		       owners[0]);
	if (ret < 0) {
		ret = -ENOMEM;
		goto free_owners;
	}

	ret = rbd_metadata_set(state->image, TCMU_RBD_LOCKER_TAG_KEY,
			       tcmu_rbd_tag);
	free(tcmu_rbd_tag);
	if (ret)
		tcmu_dev_err(dev, "Could not store lock tag. Err %d.\n", ret);

free_owners:
	if (num_owners)
		rbd_lock_get_owners_cleanup(owners, num_owners);
	return ret;
}

static int tcmu_rbd_unlock(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret;

	ret = tcmu_rbd_has_lock(dev);
	if (ret == 0)
		return TCMU_STS_OK;
	else if (ret < 0)
		return tcmu_rbd_to_sts(ret);

	ret = rbd_lock_release(state->image);
	if (!ret)
		return TCMU_STS_OK;

	tcmu_dev_err(dev, "Could not release lock. Err %d.\n", ret);
	return tcmu_rbd_to_sts(ret);
}

static int tcmu_rbd_lock(struct tcmu_device *dev, uint16_t tag)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret;

	ret = tcmu_rbd_has_lock(dev);
	if (ret == 1) {
		ret = 0;
		/*
		 * We might have failed after getting the lock, but
		 * before we set the meta data.
		 */
		goto set_lock_tag;
	} else if (ret)
		goto done;

	ret = tcmu_rbd_lock_break(dev);
	if (ret)
		goto done;

	ret = rbd_lock_acquire(state->image, RBD_LOCK_MODE_EXCLUSIVE);
	if (ret)
		goto done;

set_lock_tag:
	tcmu_dev_warn(dev, "Acquired exclusive lock.\n");
	if (tag != TCMU_INVALID_LOCK_TAG)
		ret = tcmu_rbd_set_lock_tag(dev, tag);

done:
	tcmu_rbd_service_status_update(dev, ret == 0 ? true : false);
	return tcmu_rbd_to_sts(ret);
}

static void tcmu_rbd_check_excl_lock_enabled(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	uint64_t features = 0;
	int ret;

	ret = rbd_get_features(state->image, &features);
	if (ret) {
		tcmu_dev_warn(dev, "Could not get rbd features. HA may not be supported. Err %d.\n", ret);
		return;
	}

	if (!(features & RBD_FEATURE_EXCLUSIVE_LOCK)) {
		tcmu_dev_warn(dev, "exclusive-lock not enabled for image. HA not supported.\n");
	}
}

#else /* RBD_LOCK_ACQUIRE_SUPPORT */

static void tcmu_rbd_check_excl_lock_enabled(struct tcmu_device *dev)
{
	tcmu_dev_warn(dev, "HA not supported.\n");
}

#endif /* RBD_LOCK_ACQUIRE_SUPPORT */

static void tcmu_rbd_state_free(struct tcmu_rbd_state *state)
{
	if (state->conf_path)
		free(state->conf_path);
	if (state->osd_op_timeout)
		free(state->osd_op_timeout);
	if (state->image_name)
		free(state->image_name);
	if (state->pool_name)
		free(state->pool_name);
	if (state->id)
		free(state->id);
	free(state);
}

static int tcmu_rbd_check_image_size(struct tcmu_device *dev, uint64_t new_size)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	uint64_t rbd_size;
	int ret;

	ret = rbd_get_size(state->image, &rbd_size);
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not get rbd size from cluster. Err %d.\n",
			     ret);
		return ret;
	}

	if (new_size != rbd_size) {
		tcmu_dev_err(dev, "Mismatched sizes. RBD image size %" PRIu64 ". Requested new size %" PRIu64 ".\n",
			     rbd_size, new_size);
		return -EINVAL;
	}

	return 0;
}

static int tcmu_rbd_open(struct tcmu_device *dev, bool reopen)
{
	rbd_image_info_t image_info;
	char *pool, *name, *next_opt;
	char *config, *dev_cfg_dup;
	struct tcmu_rbd_state *state;
	uint32_t max_blocks;
	int ret;

	state = calloc(1, sizeof(*state));
	if (!state)
		return -ENOMEM;
	tcmur_dev_set_private(dev, state);

	dev_cfg_dup = strdup(tcmu_dev_get_cfgstring(dev));
	config = dev_cfg_dup;
	if (!config) {
		ret = -ENOMEM;
		goto free_state;
	}

	tcmu_dev_dbg(dev, "tcmu_rbd_open config %s block size %u num lbas %" PRIu64 ".\n",
		     config, tcmu_dev_get_block_size(dev),
		     tcmu_dev_get_num_lbas(dev));

	config = strchr(config, '/');
	if (!config) {
		tcmu_dev_err(dev, "no configuration found in cfgstring\n");
		ret = -EINVAL;
		goto free_config;
	}
	config += 1; /* get past '/' */

	pool = strtok(config, "/");
	if (!pool) {
		tcmu_dev_err(dev, "Could not get pool name\n");
		ret = -EINVAL;
		goto free_config;
	}
	state->pool_name = strdup(pool);
	if (!state->pool_name) {
		ret = -ENOMEM;
		tcmu_dev_err(dev, "Could not copy pool name\n");
		goto free_config;
	}

	name = strtok(NULL, ";");
	if (!name) {
		tcmu_dev_err(dev, "Could not get image name\n");
		ret = -EINVAL;
		goto free_config;
	}

	state->image_name = strdup(name);
	if (!state->image_name) {
		ret = -ENOMEM;
		tcmu_dev_err(dev, "Could not copy image name\n");
		goto free_config;
	}

	/* The next options are optional */
	next_opt = strtok(NULL, ";");
	while (next_opt) {
		if (!strncmp(next_opt, "osd_op_timeout=", 15)) {
			state->osd_op_timeout = strdup(next_opt + 15);
			if (!state->osd_op_timeout ||
			    !strlen(state->osd_op_timeout)) {
				ret = -ENOMEM;
				tcmu_dev_err(dev, "Could not copy osd op timeout.\n");
				goto free_config;
			}
		} else if (!strncmp(next_opt, "conf=", 5)) {
			state->conf_path = strdup(next_opt + 5);
			if (!state->conf_path || !strlen(state->conf_path)) {
				ret = -ENOMEM;
				tcmu_dev_err(dev, "Could not copy conf path.\n");
				goto free_config;
			}
		} else if (!strncmp(next_opt, "id=", 3)) {
			state->id = strdup(next_opt + 3);
			if (!state->id || !strlen(state->id)) {
				ret = -ENOMEM;
				tcmu_dev_err(dev, "Could not copy id.\n");
				goto free_config;
			}
		}
		next_opt = strtok(NULL, ";");
	}

	ret = tcmu_rbd_image_open(dev);
	if (ret < 0) {
		goto free_config;
	}

	tcmu_rbd_check_excl_lock_enabled(dev);

	ret = tcmu_rbd_check_image_size(dev, tcmu_dev_get_block_size(dev) *
					tcmu_dev_get_num_lbas(dev));
	if (ret) {
		goto stop_image;
	}

	ret = rbd_stat(state->image, &image_info, sizeof(image_info));
	if (ret < 0) {
		tcmu_dev_err(dev, "Could not stat image.\n");
		goto stop_image;
	}

	/*
	 * librbd/ceph can better split and align unmaps and internal RWs, so
	 * just have runner pass the entire cmd to us. To try and balance
	 * overflowing the OSD/ceph side queues with discards/RWs limit it to
	 * up to 4.
	 */
	max_blocks = (image_info.obj_size * 4) / tcmu_dev_get_block_size(dev);
	tcmu_dev_set_opt_xcopy_rw_len(dev, max_blocks);
	tcmu_dev_set_max_unmap_len(dev, max_blocks);
	tcmu_dev_set_opt_unmap_gran(dev, image_info.obj_size /
				    tcmu_dev_get_block_size(dev), false);
	tcmu_dev_set_write_cache_enabled(dev, 0);

    ret = tcmu_rbd_setup_aio_poll(dev, tcmu_rbd_aio_epfd);
    if (ret) {
        goto stop_image;
    }

	free(dev_cfg_dup);
	return 0;

stop_image:
	tcmu_rbd_image_close(dev);
free_config:
	free(dev_cfg_dup);
free_state:
	tcmu_rbd_state_free(state);
	return ret;
}

static void tcmu_rbd_close(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);

	tcmu_rbd_teardown_aio_poll(dev, tcmu_rbd_aio_epfd);

	tcmu_rbd_image_close(dev);
	tcmu_rbd_state_free(state);
}

static int tcmu_rbd_handle_blacklisted_cmd(struct tcmu_device *dev)
		{
	tcmu_notify_lock_lost(dev);
	/*
	 * This will happen during failback normally, because
	 * running IO is failed due to librbd's immediate blacklisting
	 * during lock acquisition on a higher priority path.
	 */
	return TCMU_STS_BUSY;
}

/*
 * TODO: Check timers.
 * The rados osd op timeout must be longer than the timeouts to detect
 * unreachable OSDs (osd heartbeat grace + osd heartbeat interval) or
 * we will end up failing the transport connection when we just needed
 * to try a different OSD.
 */
static int tcmu_rbd_handle_timedout_cmd(struct tcmu_device *dev)
		{
	tcmu_dev_err(dev, "Timing out cmd.\n");
	tcmu_notify_conn_lost(dev);

	/*
	 * TODO: For AA, we will want to kill the ceph tcp connections
	 * with LINGER on and set to 0, so there are no TCP retries,
	 * and we need something on the OSD side to drop requests
	 * that end up reaching it after the initiator's failover/recovery
	 * timeout. For implicit and explicit FO, we will just disable
	 * the iscsi port, and let the initiator switch paths which will
	 * result in us getting blacklisted, so fail with a retryable
	 * error.
	 */
	return TCMU_STS_TIMEOUT;
}

#ifdef RBD_IOVEC_SUPPORT

static rbd_image_t tcmu_dev_to_image(struct tcmu_device *dev)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	return state->image;
}

#define tcmu_rbd_aio_read(dev, aio_cb, completion, iov, iov_cnt, length, offset) \
	rbd_aio_readv(tcmu_dev_to_image(dev), iov, iov_cnt, offset, completion);

#define tcmu_rbd_aio_write(dev, aio_cb, completion, iov, iov_cnt, length, offset) \
	rbd_aio_writev(tcmu_dev_to_image(dev), iov, iov_cnt, offset, completion);

#else

static int tcmu_rbd_aio_read(struct tcmu_device *dev, struct rbd_aio_cb *aio_cb,
			     rbd_completion_t completion, struct iovec *iov,
			     size_t iov_cnt, size_t length, off_t offset)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret;

	aio_cb->bounce_buffer = malloc(length);
	if (!aio_cb->bounce_buffer) {
		tcmu_dev_err(dev, "Could not allocate bounce buffer.\n");
		return -ENOMEM;
	}

	ret = rbd_aio_read(state->image, offset, length, aio_cb->bounce_buffer,
			   completion);
	if (ret < 0)
		free(aio_cb->bounce_buffer);
	return ret;
}

static int tcmu_rbd_aio_write(struct tcmu_device *dev, struct rbd_aio_cb *aio_cb,
			      rbd_completion_t completion, struct iovec *iov,
			      size_t iov_cnt, size_t length, off_t offset)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	int ret;

	aio_cb->bounce_buffer = malloc(length);
	if (!aio_cb->bounce_buffer) {
		tcmu_dev_err(dev, "Failed to allocate bounce buffer.\n");
		return -ENOMEM;;
	}

	tcmu_memcpy_from_iovec(aio_cb->bounce_buffer, length, iov, iov_cnt);

	ret = rbd_aio_write(state->image, offset, length, aio_cb->bounce_buffer,
			    completion);
	if (ret < 0)
		free(aio_cb->bounce_buffer);
	return ret;
}

#endif

/*
 * NOTE: RBD async APIs almost always return 0 (success), except
 * when allocation (via new) fails - which is not caught. So,
 * the only errno we've to bother about as of now are memory
 * allocation errors.
 */
static void rbd_finish_aio_generic(rbd_completion_t completion,
				   struct rbd_aio_cb *aio_cb)
{
	struct tcmu_device *dev = aio_cb->dev;
	struct tcmur_cmd *tcmur_cmd = aio_cb->tcmur_cmd;
	struct iovec *iov = aio_cb->iov;
	size_t iov_cnt = aio_cb->iov_cnt;
	uint32_t cmp_offset;
	int64_t ret;
	int tcmu_r;

	ret = rbd_aio_get_return_value(completion);
	rbd_aio_release(completion);

	if (ret == -ETIMEDOUT) {
		tcmu_r = tcmu_rbd_handle_timedout_cmd(dev);
	} else if (ret == -ESHUTDOWN || ret == -EROFS) {
		tcmu_r = tcmu_rbd_handle_blacklisted_cmd(dev);
	} else if (ret == -EILSEQ && aio_cb->type == RBD_AIO_TYPE_CAW) {
		cmp_offset = aio_cb->caw.miscompare_offset - aio_cb->caw.offset;
		tcmu_dev_dbg(dev, "CAW miscompare at offset %u.\n", cmp_offset);

		tcmu_r = TCMU_STS_MISCOMPARE;
		tcmu_sense_set_info(tcmur_cmd->lib_cmd->sense_buf, cmp_offset);
	} else if (ret == -EINVAL) {
		tcmu_dev_err(dev, "Invalid IO request.\n");
		tcmu_r = TCMU_STS_INVALID_CDB;
	} else if (ret < 0) {
		tcmu_dev_err(dev, "Got fatal IO error %"PRId64".\n", ret);

		if (aio_cb->type == RBD_AIO_TYPE_READ)
			tcmu_r = TCMU_STS_RD_ERR;
		else
			tcmu_r = TCMU_STS_WR_ERR;
	} else {
		tcmu_r = TCMU_STS_OK;
		if (aio_cb->type == RBD_AIO_TYPE_READ &&
		    aio_cb->bounce_buffer) {
			tcmu_memcpy_into_iovec(iov, iov_cnt,
					       aio_cb->bounce_buffer,
					       aio_cb->read.length);
		}
	}

	tcmur_cmd_complete(dev, tcmur_cmd, tcmu_r);

	if (aio_cb->bounce_buffer)
		free(aio_cb->bounce_buffer);
	free(aio_cb);
}

static int tcmu_rbd_read(struct tcmu_device *dev, struct tcmur_cmd *tcmur_cmd,
			     struct iovec *iov, size_t iov_cnt, size_t length,
			     off_t offset)
{
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->type = RBD_AIO_TYPE_READ;
	aio_cb->read.length = length;
	aio_cb->tcmur_cmd = tcmur_cmd;
	aio_cb->iov = iov;
	aio_cb->iov_cnt = iov_cnt;

	ret = rbd_aio_create_completion
		(aio_cb, NULL, &completion);
	if (ret < 0) {
		goto out_free_aio_cb;
	}

	ret = tcmu_rbd_aio_read(dev, aio_cb, completion, iov, iov_cnt,
				length, offset);
	if (ret < 0)
		goto out_release_tracked_aio;

	return TCMU_STS_OK;

out_release_tracked_aio:
	rbd_aio_release(completion);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}

static int tcmu_rbd_write(struct tcmu_device *dev, struct tcmur_cmd *tcmur_cmd,
			  struct iovec *iov, size_t iov_cnt, size_t length,
			  off_t offset)
{
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->type = RBD_AIO_TYPE_WRITE;
	aio_cb->tcmur_cmd = tcmur_cmd;

	ret = rbd_aio_create_completion
		(aio_cb, NULL, &completion);
	if (ret < 0) {
		goto out_free_aio_cb;
	}

	ret = tcmu_rbd_aio_write(dev, aio_cb, completion, iov, iov_cnt,
				 length, offset);
	if (ret < 0) {
		goto out_release_tracked_aio;
	}

	return TCMU_STS_OK;

out_release_tracked_aio:
	rbd_aio_release(completion);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}

#ifdef RBD_DISCARD_SUPPORT
static int tcmu_rbd_unmap(struct tcmu_device *dev, struct tcmur_cmd *tcmur_cmd,
			  uint64_t off, uint64_t len)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->tcmur_cmd = tcmur_cmd;
	aio_cb->type = RBD_AIO_TYPE_WRITE;
	aio_cb->bounce_buffer = NULL;

	ret = rbd_aio_create_completion
		(aio_cb, NULL, &completion);
	if (ret < 0)
		goto out_free_aio_cb;

	ret = rbd_aio_discard(state->image, off, len, completion);
	if (ret < 0)
		goto out_remove_tracked_aio;

	return TCMU_STS_OK;

out_remove_tracked_aio:
	rbd_aio_release(completion);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}
#endif /* RBD_DISCARD_SUPPORT */

#ifdef LIBRBD_SUPPORTS_AIO_FLUSH

static int tcmu_rbd_flush(struct tcmu_device *dev, struct tcmur_cmd *tcmur_cmd)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->tcmur_cmd = tcmur_cmd;
	aio_cb->type = RBD_AIO_TYPE_WRITE;
	aio_cb->bounce_buffer = NULL;

	ret = rbd_aio_create_completion
		(aio_cb, NULL, &completion);
	if (ret < 0) {
		goto out_free_aio_cb;
	}

	ret = rbd_aio_flush(state->image, completion);
	if (ret < 0) {
		goto out_remove_tracked_aio;
	}

	return TCMU_STS_OK;

out_remove_tracked_aio:
	rbd_aio_release(completion);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}

#endif

#ifdef RBD_WRITE_SAME_SUPPORT
static int tcmu_rbd_aio_writesame(struct tcmu_device *dev,
				  struct tcmur_cmd *tcmur_cmd,
				  uint64_t off, uint64_t len,
				  struct iovec *iov, size_t iov_cnt)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	size_t length = tcmu_iovec_length(iov, iov_cnt);
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->tcmur_cmd = tcmur_cmd;
	aio_cb->type = RBD_AIO_TYPE_WRITE;

	aio_cb->bounce_buffer = malloc(length);
	if (!aio_cb->bounce_buffer) {
		tcmu_dev_err(dev, "Failed to allocate bounce buffer.\n");
		goto out_free_aio_cb;
	}

	tcmu_memcpy_from_iovec(aio_cb->bounce_buffer, length, iov, iov_cnt);

	ret = rbd_aio_create_completion
		(aio_cb, NULL, &completion);
	if (ret < 0)
		goto out_free_bounce_buffer;

	tcmu_dev_dbg(dev, "Start write same off:%"PRIu64", len:%"PRIu64"\n", off, len);

	ret = rbd_aio_writesame(state->image, off, len, aio_cb->bounce_buffer,
				length, completion, 0);
	if (ret < 0)
		goto out_remove_tracked_aio;

	return TCMU_STS_OK;

out_remove_tracked_aio:
	rbd_aio_release(completion);
out_free_bounce_buffer:
	free(aio_cb->bounce_buffer);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}
#endif /* RBD_WRITE_SAME_SUPPORT */

#ifdef RBD_COMPARE_AND_WRITE_SUPPORT
static int tcmu_rbd_aio_caw(struct tcmu_device *dev, struct tcmur_cmd *tcmur_cmd,
			    uint64_t off, uint64_t len, struct iovec *iov,
			    size_t iov_cnt)
{
	struct tcmu_rbd_state *state = tcmur_dev_get_private(dev);
	struct rbd_aio_cb *aio_cb;
	rbd_completion_t completion;
	uint64_t buffer_length = 2 * len;
	ssize_t ret;

	aio_cb = calloc(1, sizeof(*aio_cb));
	if (!aio_cb) {
		tcmu_dev_err(dev, "Could not allocate aio_cb.\n");
		goto out;
	}

	aio_cb->dev = dev;
	aio_cb->tcmur_cmd = tcmur_cmd;
	aio_cb->type = RBD_AIO_TYPE_CAW;
	aio_cb->caw.offset = off;

	aio_cb->bounce_buffer = malloc(buffer_length);
	if (!aio_cb->bounce_buffer) {
		tcmu_dev_err(dev, "Failed to allocate bounce buffer.\n");
		goto out_free_aio_cb;
	}

	/* compare followed by write buffer are combined */
	tcmu_memcpy_from_iovec(aio_cb->bounce_buffer, buffer_length, iov,
			       iov_cnt);

	ret = rbd_aio_create_completion(
		aio_cb, NULL, &completion);
	if (ret < 0) {
		goto out_free_bounce_buffer;
	}

	tcmu_dev_dbg(dev, "Start CAW off: %"PRIu64", len: %"PRIu64"\n",
		     off, len);
	ret = rbd_aio_compare_and_write(state->image, off, len,
					aio_cb->bounce_buffer,
					aio_cb->bounce_buffer + len, completion,
					&aio_cb->caw.miscompare_offset, 0);
	if (ret < 0)
		goto out_remove_tracked_aio;

	return TCMU_STS_OK;

out_remove_tracked_aio:
	rbd_aio_release(completion);
out_free_bounce_buffer:
	free(aio_cb->bounce_buffer);
out_free_aio_cb:
	free(aio_cb);
out:
	return TCMU_STS_NO_RESOURCE;
}
#endif /* RBD_COMPARE_AND_WRITE_SUPPORT */

/*
 * Return scsi status or TCMU_STS_NOT_HANDLED
 */
static int tcmu_rbd_handle_cmd(struct tcmu_device *dev,
			       struct tcmur_cmd *tcmur_cmd)
{
	struct tcmulib_cmd *cmd = tcmur_cmd->lib_cmd;
	uint8_t *cdb = cmd->cdb;
	int ret;

	switch(cdb[0]) {
#ifdef RBD_WRITE_SAME_SUPPORT
	case WRITE_SAME:
	case WRITE_SAME_16:
		ret = tcmur_handle_writesame(dev, tcmur_cmd,
					     tcmu_rbd_aio_writesame);
		break;
#endif
#ifdef RBD_COMPARE_AND_WRITE_SUPPORT
	case COMPARE_AND_WRITE:
		ret = tcmur_handle_caw(dev, tcmur_cmd, tcmu_rbd_aio_caw);
		break;
#endif
	default:
		ret = TCMU_STS_NOT_HANDLED;
	}

	return ret;
}

static int tcmu_rbd_reconfig(struct tcmu_device *dev,
			     struct tcmulib_cfg_info *cfg)
{
	switch (cfg->type) {
	case TCMULIB_CFG_DEV_SIZE:
		/*
		 * Apps will already have resized on the ceph side, so no
		 * need to double check and have to also handle unblacklisting
		 * the client from this context.
		 */
		return 0;
	case TCMULIB_CFG_DEV_CFGSTR:
	case TCMULIB_CFG_WRITE_CACHE:
	default:
		return -EOPNOTSUPP;
	}
}

/*
 * For backstore creation
 *
 * Specify poolname/devicename[;option1;option2;...], e.g,
 *
 * $ targetcli /backstores/user:rbd create test 2G rbd/test[;osd_op_timeout=30]
 *
 * poolname must be the name of an existing rados pool.
 *
 * devicename is the name of the rbd image.
 */
static const char tcmu_rbd_cfg_desc[] =
	"RBD config string is of the form:\n"
	"poolname/devicename[;option1;option2;...]\n"
	"where:\n"
	"poolname:	Existing RADOS pool\n"
	"devicename:	Name of the RBD image\n"
	"optionN:	Like: \"osd_op_timeout=30\" in secs\n"
	"                     \"conf=/etc/ceph/cluster.conf\"\n"
	"                     \"id=user\"\n";

struct tcmur_handler tcmu_rbd_handler = {
	.name	       = "Ceph RBD handler",
	.subtype       = "rbd",
	.cfg_desc      = tcmu_rbd_cfg_desc,
	.open	       = tcmu_rbd_open,
	.close	       = tcmu_rbd_close,
	.read	       = tcmu_rbd_read,
	.write	       = tcmu_rbd_write,
	.reconfig      = tcmu_rbd_reconfig,
#ifdef LIBRBD_SUPPORTS_AIO_FLUSH
	.flush	       = tcmu_rbd_flush,
#endif
#ifdef RBD_DISCARD_SUPPORT
	.unmap         = tcmu_rbd_unmap,
#endif
	.handle_cmd    = tcmu_rbd_handle_cmd,
#ifdef RBD_LOCK_ACQUIRE_SUPPORT
	.lock          = tcmu_rbd_lock,
	.unlock        = tcmu_rbd_unlock,
	.get_lock_tag  = tcmu_rbd_get_lock_tag,
	.get_lock_state = tcmu_rbd_get_lock_state,
#endif
};

int handler_init(void)
{
    if (tcmu_rbd_handler_init() < 0)
        return -1;

	return tcmur_register_handler(&tcmu_rbd_handler);
}
