#!/bin/bash

# NEED TO BE RUNNED AS ROOT

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

# patching /etc/my.cnf
if [ -f /etc/redhat-release ]
    then
    perl -i.bak -pe 's/user=mysql//' /etc/my.cnf;
elif [ -f /etc/debian_version ]
    then
    perl -i.bak -pe 's/user=mysql//' /etc/mysql/my.cnf;
fi

mkdir -p $QSERV_BASE;
chown -R $QSERV_USER:$QSERV_USER $QSERV_BASE;
if [ -n "${QSERV_LOG}" ]; then
	mkdir -p $QSERV_LOG;
	chown -R $QSERV_USER:$QSERV_USER $QSERV_LOG;
fi
if [ -n "${QSERV_MYSQL_DATA}" ]; then
	mkdir -p $QSERV_MYSQL_DATA;
	chown -R $QSERV_USER:$QSERV_USER $QSERV_MYSQL_DATA;
fi
