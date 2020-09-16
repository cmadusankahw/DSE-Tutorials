
-- step 1

-- Reading ClickStream Data from CSV
clickData = LOAD 'src/resources/clickstream.csv' USING PigStorage(',') AS 
            (customer_atg_id:long, date_visit:chararray, event:chararray, c1:chararray, c2:chararray);

DESCRIBE clickData;

-- drop unwanted columns (c1 and c2) by ordering only required columns 
sortedClickData = ORDER clickData BY customer_atg_id , date_visit ASC, event ASC;

-- another way to drop unwanted columns without sorting
filteredClickData = FOREACH clickData GENERATE
     customer_atg_id, date_visit, event;

-- DUMP sortedClickData;
DUMP filteredClickData;

-- continue with the filteredData 

-- Group data by customer_atg_id
groupedClickData = GROUP data BY customer_atg_id;

-- count no of visits by each cutomer
finalClickData = FOREACH groupedData GENERATE COUNT(groupedData.event) AS (no_of_visits:double);

-- step 2

-- Reading Customer Data from CSV
custData = LOAD 'src/resources/customer_data.csv' USING PigStorage(',') AS 
            (customer_atg_id:long, customer_name:chararray);

DESCRIBE custData;

joinedData = JOIN finalClickData BY (customer_atg_id),custData by (customer_atg_id);

joinedFilteredData = FOREACH joinedData GENERATE 
    finalClickData::customer_atg_id,custData::customer_name,finalClickData::no_of_visits;

finalData = DISTINCT joinedFilteredData;

DUMP finalData;

-- step 3

-- generating top 10 data
topTenData = FOREACH finalData {                          
   DA = ORDER finalData BY no_of_visits DESC;                
   DB = LIMIT DA 10;                         
   GENERATE FLATTEN(DB.customer_atg_id), FLATTEN(DB.customer_name), FLATTEN(DB.no_of_visits);
}

DUMP topTenData;

-- store processed data into a CSV
STORE topTenData INTO 'target/exercise01/exercise01.csv' USING igStorage(',','-schema');