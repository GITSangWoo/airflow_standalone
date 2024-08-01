#!/bin/bash
user="root"
password="abcd1234"
database="history_db"
datetime=$1

mysql --local-infile=1  -u"$user" -p"$password"   <<EOF
use history_db
DELETE FROM history_db.cmd_usage WHERE dt='${datetime}';
INSERT INTO cmd_usage
SELECT 
	CASE WHEN dt LIKE '%-%-%'
		THEN STR_TO_DATE(dt, '%Y-%m-%d')
		ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d')
	END AS dt,
	command,
	CASE WHEN cnt REGEXP '[0-9]+$' 
		THEN CAST(cnt AS UNSIGNED)
		ELSE -1
	END AS cnt,
	'${datetime}' AS tmp_dt
FROM tmp_cmd_usage
WHERE dt = '${datetime}'
;
EOF
echo "DONE"
