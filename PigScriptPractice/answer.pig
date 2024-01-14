--Load in the dataset from working local directory and assign data types to columns. 
dataset = LOAD 'testDataset.csv' USING PigStorage(',') AS (id:int, name:chararray, host_id:int, host_name:chararray, neighbourhood_group:chararray, neighbourhood:chararray, 
                                                        latitude:float, longitude:float, room_type:chararray, price:int, minimum_nights:int, number_of_reviews:int, last_review:chararray,
                                                        reviews_per_month:float, calculated_host_listings_count:int, availability_365:int); --DUMP dataset;


-- Analyze records where: min_nights > 10  |  num_reviews > 10  |  2018 < last_review < 2019
subset = FILTER dataset BY minimum_nights > 10 AND number_of_reviews > 10 AND last_review MATCHES '.*2018.*|.*2019.*'; --DUMP subset;

-- Average price of each neighbourhood_group in col 2, Average num of days at each neighbourhood_group in col 3
neighbourhood_subset = GROUP subset BY neighbourhood_group;
averaged_stats_by_group = FOREACH neighbourhood_subset GENERATE group, AVG(subset.price) AS price_ordering, AVG(subset.availability_365);

-- Order results by price descending 
price_desc_avg_stats_by_group = ORDER averaged_stats_by_group BY price_ordering DESC; --DUMP price_desc_avg_stats_by_group 

-- Save to folder AirBnB_neighbourhood in home directory --> going to save in my current hdfs working directory
STORE price_desc_avg_stats_by_group INTO 'AirBnB_neighbourhood' USING PigStorage(',');

-- For subset in step 1 dump: room_type, lowest_price foreach room_type, name of property with lowest price for room_type
room_type_subset = GROUP subset BY room_type;

lowest_priced_rooms = FOREACH room_type_subset {
    price_asc_by_room_type = ORDER subset BY price ASC; --order to get the lowest price
    lowest_priced_room_type = LIMIT price_asc_by_room_type 1; --lowest price for each room type (first index of group)
    GENERATE group, FLATTEN(lowest_priced_room_type.price), FLATTEN(lowest_priced_room_type.name); --Flatten to remove tuple issue
}

DUMP lowest_priced_rooms;