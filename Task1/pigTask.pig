/* hdfs dfs -copyFromLocal clickstream.txt */
/* Task 1 - Activity Count by each user */

activity= LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/clickstream.txt' USING PigStorage('\t') as (cus_id:int,date:chararray,event:chararray);

group_data = Group activity BY cus_id;

count_by_visitor = foreach group_data Generate activity.cus_id,COUNT(activity.cus_id);

Dump count_by_visitor


/* Task 2 */
/* hdfs dfs -copyFromLocal users.txt */

users = LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/users.txt' USING PigStorage('\t') as (cus_id:int,cus_name:chararray);

names_joined = JOIN activity BY cus_id, users By cus_id;

DUMP names_joined

grouped = GROUP names_joined BY cus_name;

count_by_name = foreach grouped  Generate names_joined.cus_name,COUNT(names_joined.cus_name);

describe count_by_name;

top_users = ORDER count_by_name BY $1 DESC;

top_10_users = LIMIT top_users 10;

DUMP top_10_users;

STORE top_10_users INTO 'hdfs://quickstart.cloudera:8020/user/cloudera/out.csv' USING PigStorage(',');


