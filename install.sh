#!/bin/bash -e

systemctl stop tcmu-runner > /dev/null 2>&1

install -d /usr/lib/tcmu-runner
install handler_* /usr/lib64/tcmu-runner
install libtcmu.so.1 /usr/lib/
install libtcmu.so /usr/lib/

install tcmu-runner /usr/bin/
install tcmu-runner.conf /etc/dbus-1/system.d/
install org.kernel.TCMUService1.service /usr/share/dbus-1/system-services/
install tcmu-runner.service /lib/systemd/system/

systemctl daemon-reload

echo "OK!"
