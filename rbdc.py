#!/usr/bin/env python
# -*- coding: utf-8 -*-
# runsisi AT hust.edu.cn

import sys
import errno
import argparse
import logging
import logging.handlers

import rados
import rbd

LOG = logging.getLogger('rbdc')

DEFAULT_BLOCK_SIZE = 1048576   # bytes
RBD_IMAGE_LOCK_COOKIE = 'runsisi AT hust.edu.cn'

# TODO: support user provided ceph cluster parameters
# TODO: if rbd image has no feature RBD_FEATURE_EXCLUSIVE_LOCK, lock it
# before we write or other operations.


def main():
    sh = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)-6s] '
                                  '%(message)s')
    sh.setFormatter(formatter)
    sh.setLevel(logging.WARNING)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(sh)

    args = parse_args()

    if args.verbose:
        sh.setLevel(logging.DEBUG)

    # get size in byte
    size = None

    try:
        size = get_size(args.size)
    except ValueError as e:
        LOG.error(e)
        return errno.EINVAL

    ifname = args.ifname
    ofname = args.ofname

    if ifname == ofname:
        LOG.error('input file and output file must be different')
        return errno.EINVAL

    ifisfile = False
    ofisfile = False

    if not ifname.startswith('rbd:'):
        ifisfile = True
    else:
        ifname = ifname[4:]
        if len(ifname.split('/')) != 2:
            LOG.error('image name must in the form of rbd:poolname/imagename')
            return errno.EINVAL

    if not ofname.startswith('rbd:'):
        ofisfile = True
    else:
        ofname = ofname[4:]
        if len(ofname.split('/')) != 2:
            LOG.error('image name must in the form of rbd:poolname/imagename')
            return errno.EINVAL

    if ifisfile and ofisfile:
        return do_file2file(ifname, ofname, size)

    if ifisfile and not ofisfile:
        return do_file2rbd(ifname, ofname, size)

    if not ifisfile and ofisfile:
        return do_rbd2file(ifname, ofname, size)

    if not ifisfile and not ofisfile:
        return do_rbd2rbd(ifname, ofname, size)

    assert 0
    return errno.EINVAL


def do_file2file(ifname, ofname, size):
    rsize = 0

    try:
        with open(ifname, 'rb') as ifh:
            with open(ofname, 'wb') as ofh:
                while True:
                    sz = min(size, DEFAULT_BLOCK_SIZE)
                    if sz == 0:
                        break
                    data = ifh.read(sz)
                    if not data:
                        break
                    # TODO: what if not finished as expected?
                    ofh.write(data)

                    size -= sz
                    rsize += sz
    except IOError as e:
        LOG.error(e)
        return e.errno

    return 0, rsize


def do_file2rbd(ifname, ofname, size):
    (poolname, imagename) = ofname.split('/')
    rsize = 0

    try:
        with open(ifname, 'rb') as ifh:
            with rados.Rados(conffile='') as client:
                with client.open_ioctx(poolname) as ioctx:
                    with rbd.Image(ioctx, imagename) as image:
                        rsize = size = min(size, image.size())
                        offset = 0
                        while True:
                            sz = min(size, DEFAULT_BLOCK_SIZE)
                            if sz == 0:
                                break
                            data = ifh.read(sz)
                            if not data:
                                break
                            LOG.debug('writing offset: {0}, size: {1}'
                                      .format(offset, sz))
                            image.write(data, offset)
                            LOG.debug('write offset: {0} ok'.format(offset))

                            offset += sz
                            size -= sz
    except IOError as e:
        LOG.error(e)
        return e.errno or errno.EIO
    except rados.Error as e:
        LOG.error(e)
        return errno.EIO
    except rbd.Error as e:
        LOG.error(e)
        return errno.EIO

    return 0, rsize


def do_rbd2file(ifname, ofname, size):
    (poolname, imagename) = ifname.split('/')
    rsize = 0

    try:
        with open(ofname, 'wb') as ofh:
            with rados.Rados(conffile='') as client:
                with client.open_ioctx(poolname) as ioctx:
                    with rbd.Image(ioctx, imagename) as image:
                        rsize = size = min(size, image.size())
                        offset = 0
                        while True:
                            sz = min(size, DEFAULT_BLOCK_SIZE)
                            if sz == 0:
                                break
                            LOG.debug('reading offset: {0}, size: {1}'
                                      .format(offset, sz))
                            data = image.read(offset, sz)
                            LOG.debug('read offset: {0} ok'.format(offset))
                            if not data:
                                break
                            ofh.write(data)

                            offset += sz
                            size -= sz
    except IOError as e:
        LOG.error(e)
        return e.errno or errno.EIO
    except rados.Error as e:
        LOG.error(e)
        return errno.EIO
    except rbd.Error as e:
        LOG.error(e)
        return errno.EIO

    return 0, rsize


def do_rbd2rbd(ifname, ofname, size):
    (poolname1, imagename1) = ifname.split('/')
    (poolname2, imagename2) = ofname.split('/')
    rsize = 0

    try:
        with rados.Rados(conffile='') as client:
            with client.open_ioctx(poolname1) as ioctx1:
                with client.open_ioctx(poolname2) as ioctx2:
                    with rbd.Image(ioctx1, imagename1) as image1:
                        with rbd.Image(ioctx2, imagename2) as image2:
                            rsize = size = min(size, image1.size(), image2.size())
                            offset = 0
                            while True:
                                sz = min(size, DEFAULT_BLOCK_SIZE)
                                if sz == 0:
                                    break
                                LOG.debug('reading offset: {0}, size: {1}'
                                          .format(offset, sz))
                                data = image1.read(offset, sz)
                                LOG.debug('read offset: {0} ok'.format(offset))
                                if not data:
                                    break
                                LOG.debug('writing offset: {0}, size: {1}'
                                          .format(offset, sz))
                                image2.write(data, offset)
                                LOG.debug('write offset: {0} ok'.format(offset))

                                offset += sz
                                size -= sz
    except IOError as e:
        LOG.error(e)
        return e.errno or errno.EIO
    except rados.Error as e:
        LOG.error(e)
        return errno.EIO
    except rbd.Error as e:
        LOG.error(e)
        return errno.EIO

    return 0, rsize


def get_size(size):
    '''
    Get size in unit of byte.
    :param size: size string.
    :return: size in unit of byte.
    '''
    if not isinstance(size, str):
        raise ValueError('size is not a string')

    units1 = {'B': 1, 'K': 1024, 'M': 1024 * 1024, 'G': 1024 * 1024 * 1024}
    units2 = {'KB': 1024, 'MB': 1024 * 1024, 'GB': 1024 * 1024 * 1024}

    unit2 = size[-2:].upper()
    if unit2 in units2:
        count = int(size[:-2])
        return count * units2[unit2]

    unit1 = size[-1:].upper()
    if unit1 in units1:
        count = int(size[:-1])
        return count * units1[unit1]

    return int(size)


def parse_args():
    # rbdc --if /dev/urandom --of rbd:poolxxx/imageyyy 10M
    # rbdc --if rbd:poolxxx/imageyyy --of /home/runsisi/test.dat 100KB

    parser = argparse.ArgumentParser('rbdc')

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='be more verbose'
    )
    parser.add_argument(
        '-i', '--if',
        required=True,
        dest='ifname',
        metavar='ifnmae',
        help='input file, specified as: rbd:poolname/imagename if it is a rbd'
    )
    parser.add_argument(
        '-o', '--of',
        required=True,
        dest='ofname',
        metavar='ofnmae',
        help='output file, specified as: rbd:poolname/imagename if it is a rbd'
    )
    parser.add_argument(
        'size',
        help='size to read/write, in unit of B/KB/MB/GB, lower case is OK, '
             'default unit is B (byte)'
    )

    return parser.parse_args()


if __name__ == '__main__':
    import time
    from datetime import timedelta

    start = time.time()
    ret = main()
    end = time.time()

    if not isinstance(ret, tuple):
        print('Failed!!!')
    else:
        print('OK!')
        print('Copied {0} bytes.'.format(ret[1]))
        print('Elapsed time: {0}.'.format(timedelta(seconds=(end - start))))
        ret = ret[0]
    sys.exit(ret)
