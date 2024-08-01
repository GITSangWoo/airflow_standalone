CREATE DATABASE IF NOT EXISTS history_db ; 
ALTER DATABASE history_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;


USE history_db;

CREATE TABLE IF NOT EXISTS history_db.tmp_cmd_usage (
	dt VARCHAR(500),
	command VARCHAR(500) CHARACTER SET utf8mb4,
	cnt VARCHAR(500)
);

-- USE history_db;

CREATE TABLE IF NOT EXISTS history_db.cmd_usage (
	dt DATE,
	command VARCHAR(500),	
	cnt INT,
	tmp_dt VARCHAR(500)
);
