#!/bin/bash

CSV_PATH=$1

user="root"
password="abcd1234"
database="history_db"
DEL_DT=$2

mysql --local-infile=1  -u"$user" -p"$password"  <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '${CSV_PATH}'
INTO TABLE history_db.tmp_cmd_usage
character set latin1
FIELDS 
	TERMINATED BY ','
	 ENCLOSED BY '^'
	 ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
