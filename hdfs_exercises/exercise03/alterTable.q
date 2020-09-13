
SHOW TABLES;

-- With non-default Database

CREATE DATABASE alter3_db;
USE alter3_db;
SHOW TABLES;

CREATE TABLE alter3_src (col1 STRING) STORED AS TEXTFILE ;
LOAD DATA LOCAL INPATH '../../data/files/test.dat' OVERWRITE INTO TABLE alter3_src ;

CREATE TABLE alter3 (col1 STRING) PARTITIONED BY (pcol1 STRING, pcol2 STRING) STORED AS SEQUENCEFILE;

CREATE TABLE alter3_like LIKE alter3;

INSERT OVERWRITE TABLE alter3 PARTITION (pCol1='test_part:', pcol2='test_part:') SELECT col1 FROM alter3_src ;
SELECT * FROM alter3 WHERE pcol1='test_part:' AND pcol2='test_part:';

ALTER TABLE alter3 RENAME TO alter3_renamed;
DESCRIBE EXTENDED alter3_renamed;
DESCRIBE EXTENDED alter3_renamed PARTITION (pCol1='test_part:', pcol2='test_part:');
SELECT * FROM alter3_renamed WHERE pcol1='test_part:' AND pcol2='test_part:';

INSERT OVERWRITE TABLE alter3_like
PARTITION (pCol1='test_part:', pcol2='test_part:')
SELECT col1 FROM alter3_src;
ALTER TABLE alter3_like RENAME TO alter3_like_renamed;

DESCRIBE EXTENDED alter3_like_renamed;