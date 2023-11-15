/*
CP422 - A02
Author: Chandler Mayberry 190688910
Due: 2023-11-15
*/

--Load in the dataset from working local directory and assign data types to columns. 
--:::NOTE::: I used my working directory for Windows Hadoop as my path, you may have to change the path if it differs below.
dataset = LOAD 'AB_NYC_2019.csv' USING PigStorage(',') AS (id:int, name:chararray, host_id:int, host_name:chararray, neighbourhood_group:chararray, neighbourhood:chararray, 
                                                        latitude:float, longitude:float, room_type:chararray, price:int, minimum_nights:int, number_of_reviews:int, last_review:chararray,
                                                        reviews_per_month:float, calculated_host_listings_count:int, availability_365:int); --DUMP dataset;


-- 1. Analyze records where: min_nights > 10  |  num_reviews > 10  |  2018 < last_review < 2019
subset = FILTER dataset BY minimum_nights > 10 AND number_of_reviews > 10 AND last_review MATCHES '.*2018.*|.*2019.*'; --DUMP subset;




-- 2. Put the neighbourhood_group's together and store in col 1 (this is what I interpret from the question)
neighbourhood_subset = GROUP subset BY neighbourhood_group;

-- Average price of each neighbourhood_group in col 2, Average num of days at each neighbourhood_group in col 3
averaged_stats_by_group = FOREACH neighbourhood_subset GENERATE GROUP, AVG(subset.price) AS price_ordering, AVG(subset.availability_365);

-- Order results by price descending 
price_desc_avg_stats_by_group = ORDER averaged_stats_by_group BY price_ordering DESC;

-- Save to folder AirBnB_neighbourhood in home directory --> going to save in my current hdfs working directory
STORE price_desc_avg_stats_by_group INTO 'AirBnB_neighbourhood' USING PigStorage(',');



-- 3. For subset in step 1 dump: room_type, lowest_price foreach room_type, name of property with lowest price for room_type

room_type_subset = GROUP subset BY room_type;

lowest_price_by_room_type = FOREACH room_type_subset {
    lowest_price = MIN(subset.price);
    lowest_price_for_room = FILTER subset BY price = lowest_price; --keep only the lowest price per room type
    GENERATE GROUP AS room_type, lowest_price_for_room.price AS price, lowest_price_for_room AS property;
}

DUMP lowest_price_by_room_type
