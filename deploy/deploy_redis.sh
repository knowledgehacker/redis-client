#!/bin/sh

slaves="slaves"

REDIS_ROOT_DIR=$HOME/redis_cluster
REDIS_VERSION=2.8.17
REDIS_CONF_FILE=redis.conf

USER=minglin

install_redis() {
	local master_host=$1
	echo $master_host

	ssh $USER@$master_host mkdir -p $REDIS_ROOT_DIR

	#scp redis-$REDIS_VERSION.tar.gz $USER@$master_host:$REDIS_ROOT_DIR
	#ssh $USER@$master_host tar zxvf $REDIS_ROOT_DIR/redis-$REDIS_VERSION.tar.gz -C $REDIS_ROOT_DIR
	if [ ! -d "redis-$REDIS_VERSION" ]; then 
		tar zxf redis-$REDIS_VERSION.tar.gz
		make -C redis-$REDIS_VERSION	
	fi
	scp redis-$REDIS_VERSION/src/redis-server $USER@$master_host:$REDIS_ROOT_DIR

	#configure master
	echo "configure master $master_host..."
	configure_master $master_host 0

	# configure slaves
	local i=1
	while read slave_host
	do
		echo "configure slave $slave_host..."
		configure_slave $master_host $slave_host $i
		i=`expr $i + 1`
	done < $slaves
}

configure_master() {
	local master_host=$1
	local id=$2

	cp $REDIS_CONF_FILE $id-$REDIS_CONF_FILE
    echo "bind $master_host" >> $id-$REDIS_CONF_FILE
    echo "port `expr 7000 + $id`" >> $id-$REDIS_CONF_FILE
	#configure $master_host $id

	copy_configure $master_host $id
}

configure_slave() {
	local master_host=$1
	local slave_host=$2
	local id=$3

	cp $REDIS_CONF_FILE $id-$REDIS_CONF_FILE
    echo "bind $master_host" >> $id-$REDIS_CONF_FILE
    echo "port `expr 7000 + $id`" >> $id-$REDIS_CONF_FILE
	echo "slaveof $slave_host 7000" >> $id-$REDIS_CONF_FILE

	copy_configure $master_host $id
}

copy_configure() {
	local master_host=$1
	local id=$2

	ssh $USER@$master_host mkdir -p $REDIS_ROOT_DIR/$id
	scp $id-$REDIS_CONF_FILE $USER@$master_host:$REDIS_ROOT_DIR/$id/$REDIS_CONF_FILE
	rm $id-$REDIS_CONF_FILE
}

install_redis $1
