#!/bin/bash -ex

# ceph.conf

cat <<-EOF > /etc/ceph/ceph.conf
[global]
    fsid = $(uuidgen)
    osd pg bits = 3
    osd crush chooseleaf type = 0
    osd pool default min size = 1
    osd pool default size = 1
    auth supported = cephx
    mon host = 192.168.33.10
[mon.a]
    host = $(hostname -s)
EOF

# mon. & client.admin keyring
keyring_fn=/etc/ceph/ceph.client.admin.keyring

ceph-authtool $keyring_fn --create-keyring --gen-key --name=mon. --cap mon 'allow *'
ceph-authtool $keyring_fn --gen-key --name=client.admin --set-uid=0 \
        --cap mon 'allow *' \
        --cap osd 'allow *' \
        --cap mds 'allow *'

# mon.mkfs

ceph-mon -i a --mkfs --keyring=$keyring_fn

# mon.start

service ceph start mon.a

