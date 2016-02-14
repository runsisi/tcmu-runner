#!/bin/bash -e

systemctl stop tcmu-runner > /dev/null 2>&1

install -d /usr/lib/tcmu-runner
install handler_* /usr/lib/tcmu-runner
install libtcmu.so.1 /usr/lib/
install libtcmu.so /usr/lib/

cp -f tcmu-runner /usr/bin/
cp -f tcmu-runner.conf /etc/dbus-1/system.d/
cp -f org.kernel.TCMUService1.service /usr/share/dbus-1/system-services/
cp -f tcmu-runner.service /lib/systemd/system/

systemctl daemon-reload

echo "OK!"
