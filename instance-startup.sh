#!/bin/bash

echo 'deb http://www.rabbitmq.com/debian/ testing main' >> /etc/apt/sources.list.d/rabbitmq.list
wget -O- https://www.rabbitmq.com/rabbitmq-release-signing-key.asc | apt-key add -
apt-get -y update
apt-get -y install build-essential python-dev libffi-dev libssl-dev python-pip git wget sqlite3 supervisor rabbitmq-server
pip install -U virtualenv google-api-python-client pyinotify json-spec python-dateutil pika

groupadd supervisor
chgrp -R supervisor /etc/supervisor /var/log/supervisor
chmod -R 0775 /etc/supervisor /var/log/supervisor


