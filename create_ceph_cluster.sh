#!/bin/bash
# runsisi AT hust.edu.cn

# TODO: debian/ubuntu support

MON_ID="a"
OSD_ID="0"

keyring_fn=/etc/ceph/ceph.client.admin.keyring

usage() {
    echo "Manipulate a minimal ceph cluster (1 MON, 1 OSD)"
    echo
    echo "Usage:"
    echo "    ./$(basename $0) [action]"
    echo
    echo "action:"
    echo "    create | destroy | start | stop | status"
    echo
}

# handle parameter
if [ $# -ne 1 ]; then
    usage
    exit 1
fi

check_pkgs() {
    if ! rpm -qa | grep -q '^ceph'; then
        return 1
    fi
}

check_mon() {
    local status=0 # not exist
    if [ -d /var/lib/ceph/mon/ceph-${MON_ID} ]; then
        status=1 # exist
        if service ceph status mon.${MON_ID} > /dev/null; then
            status=2 # running
        fi
    fi
    return ${status}
}

check_osd() {
    local status=0 # not exist
    if [ -d /var/lib/ceph/osd/ceph-${OSD_ID} ]; then
        status=1 # exist
        if service ceph status osd.${OSD_ID} > /dev/null; then
            status=2 # running
        fi
    fi
    return ${status}
}

ceph_create() {
    if ! check_pkgs; then
        echo "- No ceph packages installed"
        return 1
    fi

    check_mon
    local status=$?
    if [ ${status} -eq 0 ]; then
        echo "- MON.${MON_ID} not exist, create it"

        # ceph.conf
cat <<-EOF > /etc/ceph/ceph.conf
[global]
    fsid = $(uuidgen)
    osd pg bits = 5
    osd crush chooseleaf type = 0
    osd pool default min size = 1
    osd pool default size = 1
    auth supported = cephx
    mon host = 192.168.33.10
[mon.a]
    host = $(hostname -s)
EOF

        # mon. & client.admin keyring
        ceph-authtool ${keyring_fn} --create-keyring --gen-key --name=mon. --cap mon 'allow *'
        ceph-authtool ${keyring_fn} --gen-key --name=client.admin --set-uid=0 \
                --cap mon 'allow *' \
                --cap osd 'allow *' \
                --cap mds 'allow *'

        # mon.mkfs
        ceph-mon -i a --mkfs --keyring=${keyring_fn}
    else
        echo "- mon.${MON_ID} already created"
    fi

    check_osd
    status=$?
    if [ ${status} -eq 0 ]; then
        echo "- OSD.${OSD_ID} not exist, create it"

        mkdir -p /var/lib/ceph/osd/ceph-${OSD_ID}
        ceph-disk prepare /var/lib/ceph/osd/ceph-${OSD_ID}
    else
        echo "- osd.${OSD_ID} already created"
    fi
}

ceph_destroy() {
    service ceph stop
    rm -rf /var/lib/ceph/osd/ceph-${OSD_ID}
    rm -rf /var/lib/ceph/mon/ceph-${MON_ID}
    rm -f /var/lib/ceph/bootstrap-*/*
}

ceph_start() {
    service ceph start mon.${MON_ID}
    while [ ! -e /var/lib/ceph/bootstrap-osd/ceph.keyring ]; do
        echo "- Waiting osd ceph.keyring"
        sleep 1
    done
    ceph-disk activate /var/lib/ceph/osd/ceph-${OSD_ID}
}

ceph_stop() {
    service ceph stop
}

ceph_status() {
    check_mon
    local status=$?
    if [ ${status} -eq 0 ]; then
        echo "- MON.${MON_ID} not exist"
    elif [ ${status} -eq 1 ]; then
        echo "- MON.${MON_ID} exist and stopped"
    else
        echo "- MON.${MON_ID} exist and running"
    fi

    check_osd
    status=$?
    if [ ${status} -eq 0 ]; then
        echo "- OSD.${OSD_ID} not exist"
    elif [ ${status} -eq 1 ]; then
        echo "- OSD.${OSD_ID} exist and stopped"
    else
        echo "- OSD.${OSD_ID} exist and running"
    fi
}

if [ "$1" = "create" ]; then
    ceph_create
elif [ "$1" = "destroy" ]; then
    ceph_destroy
elif [ "$1" = "start" ]; then
    ceph_start
elif [ "$1" = "stop" ]; then
    ceph_stop
elif [ "$1" = "status" ]; then
    ceph_status
fi

