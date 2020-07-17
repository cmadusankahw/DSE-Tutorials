
input_file = LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/WorldCups.csv' USING PigStorage(',') as(year:int,country:chararray,winners:chararray,runnersUp:chararray,third:chararray,fourth:chararray,goals:int,qualifiedTeams:int,matchesPlayed:int,attendence:long);

ranked = RANK input_file;

NoHeader = FILTER ranked BY (rank_input_file>1);

worldcups = FOREACH NoHeader GENERATE *;

sameCountries = FILTER worldcups BY (worldcups.country == worldcups.winners) OR (worldcups.country == worldcups.runnersUp);

sameCountries_out = FOREACH sameCountries GENERATE worldcups.year , worldcups.country;

DUMP sameCountries_out;

orderGoal = ORDER worldcups BY goals DESC;

topGoal = LIMIT orderGoal 1;

top_goals = FOREACH topGoals GENERATE topGoals.year;

DUMP top_goals

topTenGoal = LIMIT orderGoal 10;

topTenGoalCOuntries = FOREACH topTenGoal GENERATE topTenGoal.year , topTenGoal.country;

DUMP topTenGoalCOuntries;

