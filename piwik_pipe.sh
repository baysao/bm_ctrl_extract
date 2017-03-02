#!/bin/sh
mydir="/data/app/bimax-counters/extract"
import_log=$mydir/import_logs1.py
tk=bce12b0011714f1836a4b4cccdbb190f
id="$1"
url="$2"
if [ $id -eq 1  ];then
	opt="--add-sites-new-hosts"
	n=1
	nn=200
else	
	opt=""
	n=4
	nn=200
fi
exec python $import_log \
 --token-auth=$tk \
 --url=http://$url $opt \
 --recorders=$n --recorder-max-payload-size=$nn --enable-http-errors --enable-http-redirects --enable-static --enable-bots \
 --log-format-name=nginx_json - 
