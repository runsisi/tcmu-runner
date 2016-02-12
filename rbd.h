/*
 * rbd.h
 *
 *  Created on: Feb 9, 2016
 *      Author: runsisi AT hust.edu.cn
 */

#ifndef RBD_H_
#define RBD_H_

enum io_data_op {
    IO_D_READ   = 0,
    IO_D_WRITE  = 1,
    IO_D_TRIM   = 2,
    IO_D_SYNC   = 3,
};

enum {
    IO_F_FREE   = 1 << 0,
    IO_F_FLIGHT = 1 << 1,
};

struct io {
    unsigned int flags;
    enum io_data_op data_op;
    unsigned long long offset;
    void *xfer_buf;
    unsigned long xfer_buflen;

    struct rbd_io_handler *h;
    struct tcmulib_cmd *cmd;
    rbd_completion_t completion;
};

#endif /* RBD_H_ */
