LOAD 'hdfs://quickstart.cloudera:8020/user/cloudera/WorldCupMatches.csv' USING PigStorage(',') as (year:int,date:chararray,stage:chararray,stadium:chararray,city:chararray,homeTeam:chararray,hGoals:int,aGoals:int,awayTeam:chararray,condition:chararray,attendance:int,halfHGoals:int,halfAGoals:int,reference:chararray,as1:chararray,as2:chararray,roundId:int,matchId:int,hInitial:chararray,aInitial:chararray);

ranked = RANK input_file;

NoHeader = FILTER ranked BY (rank_input_file>1);

match_details = FOREACH NoHeader GENERATE *;




/* Question 1*/

attendance_home = FOREACH match_details GENERATE homeTeam,attendance;

attendance_away = FOREACH match_details GENERATE awayTeam,attendance;

all_teams = UNION attendance_home,attendance_away;

grouped_teams = GROUP all_teams BY homeTeam;

sum_data = FOREACH grouped_teams GENERATE *,SUM(all_teams.attendance);

popularity_order = ORDER sum_data BY $1 DESC;

popular_teams = LIMIT popularity_order 10;

DUMP popular_teams




/* Question 2*/

winners = FOREACH match_details GENERATE *,(hGoals > aGoals ? homeTeam:awayTeam) as winner;

DESCRIBE winners

winners_top = LIMIT winners 10;

DUMP winners_top

DUMP winners

matches_in_current_month = FILTER match_details BY (date matches '.*Jul.*');

DUMP matches_in_current_month




/*Question 3 */

distinct_data = DISTINCT match_details;

DESCRIBE distinct_data

DUMP distinct_data




/*Question 4 */

home_goals = FOREACH match_details GENERATE year,homeTeam,hGoals;

away_goals = FOREACH match_details GENERATE year,awayTeam,aGoals;

all_goals = UNION home_goals,away_goals;

DESCRIBE all_goals

grouped_goals = GROUP all_goals BY (year,homeTeam);

DESCRIBE grouped_goals

sum_goals = FOREACH grouped_goals GENERATE *, SUM(all_goals.hGoals);

result = LIMIT sum_goals 10;

DUMP result












