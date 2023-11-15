Given data set for New York’s AirBnB. The columns are:

 
id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365

 
Write a Pig program to analyze the following information.

 
# Step 1: 
Analyze only the records where minimum_nights>10, number_of_reviews > 10 and the last_review falls within 2018 or 2019.


# Step 2: 
For the records from Step 1, get the general neighborhood_group information and store it with the neighbourhood_group as the 1st column, the average price of the neighbourhood_group as the 2nd column, and the average number of available days for the neighbourhood_group as the 3rd column. Order the results by price, from high to low, and save them to the folder "AirBnB_neighbourhood" in the hdfs home directory.
 

# Step 3: 
For the records from Step 1, display on the console the room_type, the lowest price for each room_type, and the name of the property with the lowest price for the room_type.



Submit answer.pig only (include your name and student ID as comments in the script).