#!/bin/sh
port=$1
dir=/data/app/bimax-counters/extract
conf=$dir/bi_extract_0.conf
proc="/usr/bin/python $dir/queue_piwik_2.1"

tmp=`mktemp`
tmpd=`mktemp -d`	
while true;do
	delay=`awk -F'=' '/piwik_queue_real_delay=/ {print $2}' $conf | head -1`
	queue0=`awk -F'=' '/piwik_queue_real_queue_name=/ {print $2}' $conf | head -1`
#	queue1=`awk -F'=' '/piwik_queue_real_queue_next=/ {print $2}' $conf | head -1`
	queue=${queue0}$port	
#	queue_next=${queue1}$port	
	log=`awk -F'=' '/piwik_queue_real_log=/ {print $2}' $conf | head -1`
	queue_host=`awk -F'=' '/piwik_queue_real_host=/ {print $2}' $conf | head -1`
	rd="/usr/bin/redis-cli $queue_host"
        filters=`cat $conf | grep "piwik_queue_real_filter_$port" | cut -d'=' -f2 | head -1`
        host=`cat $conf | grep "piwik_queue_real_host_$port"| cut -d'=' -f2 | head -1`
        requeue_cc=`awk -F'=' '/piwik_queue_real_cc=/ {print $2}' $conf | head -1`
        requeue_ll=`awk -F'=' '/piwik_queue_real_ll=/ {print $2}' $conf | head -1`
        requeue_state=`awk -F'=' '/piwik_queue_real_state=/ {print $2}' $conf | head -1`
        if [ "$requeue_state" == "pause" ];then sleep 1;continue;fi

	id=1
	nn=$((requeue_ll + 1))
	fn="`date +'%Y_%m_%d_%H_%M' --date="$delay minute ago"`"
	f="`echo "rpop $queue" | $rd`"
#                if [ -z "$f" ];then continue; fi
#                if [ ! -f "$f" ];then continue; fi
#		found=0
#		for fil in $filters;do
#			echo "$f" | grep $fil > /dev/null
#			if [ $? -eq 0 ]; then found=1;break;fi
#		done
#		if [ $found -ne 1 ];then 
#			continue
#		fi
#		dd="`echo $f | cut -d'_' -f2-`"	
#	   	if [ "$dd" \< "$fn" ];then
 #                       echo "`date`:$port ignore $f" >> $log
#			echo "rpush ${queue_next} $f" |$rd > /dev/null
#			continue
 #               fi
		echo "`date`:$port process $f" >> $log
		ll=`wc -l $f | awk -v ll=$requeue_ll  '{printf("%.f", $1/ll);}'`
		cd $tmpd
		split -l $ll $f	
		> $tmp
		find $tmpd -type f | while read ff;do
			echo $proc $ff $id $host >> $tmp
                	id=$((id + 1)) 
		done
	cmd="`cat $tmp`"	
	if [ -n "$cmd" ];then
		echo "$cmd" | parallel -j32
	fi
	rm -f $tmpd/*
	sleep 1
done
