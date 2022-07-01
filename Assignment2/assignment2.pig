-- Use CSVLoader to avoid problems with quotation marks.
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load all the data from orders.csv and give the columns the correct datatype.
orderCSV= LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVLoader(',') AS
    (
        game_id : int,
        unit_id : int, 
        unit_order : chararray, 
        location : chararray, 
        target : chararray, 
        target_dest : chararray, 
        success : int, 
        reason : chararray, 
        turn_num : int
    );

-- Filter the data by target 'Holland'
filtered_data = FILTER orderCSV BY target == 'Holland';

-- Group the data by location and generate the output.
grouped_data = FOREACH(GROUP filtered_data by location)
				GENERATE group as location, MAX(filtered_data.(target)) as target, COUNT($1) as c;

-- Sort the data by location ascending.
result = ORDER grouped_data BY location ASC;

-- Dump/show the result.
DUMP result