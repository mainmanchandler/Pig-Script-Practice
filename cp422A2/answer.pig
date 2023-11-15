/*
CP422 - A02
Author: Chandler Mayberry 190688910
Due: 2023-11-15
*/

--Load in the dataset from working local directory and assign data types to columns. 
--:::NOTE::: I used my working directory as my path, you may have to change the path if it differs below.
dataset = LOAD 'AB_NYC_2019.csv' USING PigStorage(',') AS (id:int, name:chararray, host_id:int, host_name:chararray, neighbourhood_group:chararray, neighbourhood:chararray, 
                                                        latitude:float, longitude:float, room_type:chararray, price:int, minimum_nights:int, number_of_reviews:int, last_review:chararray,
                                                        reviews_per_month:float, calculated_host_listings_count:int, availability_365:int);
--DUMP dataset


-- 1. Analyze records where min_nights > 10  |  num_reviews > 10  |  2018 < last_review < 2019
subset = FILTER dataset BY minimum_nights > 10 AND number_of_reviews > 10 AND last_review MATCHES '2018.*|2019.*'
DUMP subset


-- 2. 
