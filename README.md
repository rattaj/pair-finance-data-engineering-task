## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.

### ETL task implementation
The ETL task has been implemented in file `analytics/analytics.py`. The test cases have been implemented in `analytics/test_analytics.py`.  
The driver function is defined as `devices_data_etl` and performs below operations:
1. Read the devices data from the postgres
2. Perform the aggregations to calculate max temperature, number of data points and the device distance during the period of one hour.
3. Store the aggregated results into the MySQL database.

The file `main/main.py` has been updated to `commit` the devices_data insert record operation.  
The dockerfile `analytics/Dockerfile` is updated to the build additional required dependencies such as:
- pandas: To manipulate the data.
- geopy: To calculate the distance between two gps coordinates.
- pymysql & cryptography: To connect to the MySQL.

