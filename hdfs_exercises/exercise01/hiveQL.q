-- create a new table
CREATE TABLE guruhive_internaltable (id INT,Name STRING);
	 Row format delimited 
	 Fields terminated by '\t';

-- load data inot table from external source
LOAD DATA INPATH '/data/files/data.txt' INTO table test;

SELECT * FROM test;

DROP TABLE test;

-- creating exeternal tables
-- If processing data available in HDFS
-- Useful when the files are being used outside of Hive 
CREATE EXTERNAL TABLE guruhive_external(id INT,Name STRING)
	 Row format delimited
	 Fields terminated by '\t'
	 LOCATION '/data/files/data.txt';

-- create a partition table
CREATE TABLE state_part(District string,Enrolments string) PARTITIONED BY(state string);

-- insert data by overriding table data
INSERT OVERWRITE TABLE state_part PARTITION(state);
SELECT district,enrolments,state from  allstates;

-- Buckets in hive is used in segregating of hive table-data into multiple files or directories. it is used for efficient querying. 

-- creating views
CREATE VIEW Sample_View AS SELECT * FROM employees WHERE salary>25000;

-- Indexes are pointers to particular column name of a table. 
CREATE INDEX sample_Index ON TABLE employees(id);

-- Hive queries Order_By, Group_By, Sort_By, Distributed_By
SELECT * FROM employees ORDER BY Department;
SELECT Department, count(*) FROM employees GROUP BY Department;
SELECT * from employees SORT BY Id DESC;
SELECT  Id, Name from employees DISTRIBUTE BY Id;




