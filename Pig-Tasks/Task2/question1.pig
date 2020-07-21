
/* Read CSV File */

input_file = LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/WorldCups.csv' USING PigStorage(',') as(year:int,country:chararray,winners:chararray,runnersUp:chararray,third:chararray,fourth:chararray,goals:int,qualifiedTeams:int,matchesPlayed:int,attendence:float);

ranked = RANK input_file;

NoHeader = FILTER ranked BY (rank_input_file>1);

/* Remove Headers from the CSV File */

/* Question 1 */

worldcups = FOREACH NoHeader GENERATE *;

sameCountries = FILTER worldcups BY country == winners OR country == runnersUp;

sameCountries_out = FOREACH sameCountries GENERATE worldcups.year , worldcups.country;

DUMP sameCountries_out

/* Question 2 */

orderGoal = ORDER worldcups BY goals DESC;

topGoal = LIMIT orderGoal 1;

top_goals = FOREACH topGoals GENERATE topGoals.year;

DUMP top_goals

topTenGoal = LIMIT orderGoal 10;

topTenGoalCOuntries = FOREACH topTenGoal GENERATE topTenGoal.year , topTenGoal.country;

DUMP topTenGoalCOuntries

/* Question 3 */

grouped_world_cups = GROUP worldcups All;

average_attendance = FOREACH grouped_world_cups GENERATE worldcups.year , AVG(worldcups.attendence);

DUMP average_attendance

/*Question 4 */

group_by_country = GROUP worldcups BY (winners);

winning_count = FOREACH group_by_country GENERATE worldcups, COUNT(worldcups.winners);

Describe winning_count

order_count = ORDER winning_count BY $1 DESC;

country_with_max = LIMIT order_count 1;

DUMP country_with_max
