
-- Reading WorldCups Data from CSV
worldCups = LOAD 'src/resources/WorldCups.csv' USING PigStorage(',') AS 
            (Year:long,Country:chararray,Winner:chararray,Runners-Up:chararray,Third:chararray,Fourth:chararray,
            GoalsScored:int,QualifiedTeams:int,MatchesPlayed:int,Attendance:double);

DESCRIBE worldCups;

DUMP worldCups;

-- top 10 Goal scored countries
topTenData = FOREACH worldCups {                          
   DA = ORDER worldCups BY GoalsScored DESC;                
   DB = LIMIT DA 10;                         
   GENERATE FLATTEN(DB.Year), FLATTEN(DB.Country), FLATTEN(DB.Winner), FLATTEN(DB.GoalsScored) ;
}

--  highest goal scored year
highestYear = FOREACH topTenData {
    AA = LIMIT topTenData 1;
    GENERATE FLATTEN(AA.Year)
}

-- country with the most wins
groupdByCountry = FOREACH worldCups {
    GC = GROUP worldCups BY Winner GENERATE COUNT(worldCups.Winner)  AS (no_of_winnigs:double);
    OC = ORDER GC BY no_of_winnigs;
    FC = LIMIT OC BY 1;
}

-- avarage attendence of the worldCups
avgAttendence = FOREACH worldCups GENERATE AVG(worldCups.Attendance);

DUMP groupdByCountry;
DUMP avgAttendence;

-- store processed data into a CSV
STORE topTenData INTO 'target/exercise02/exercise02.csv' USING igStorage(',','-schema');