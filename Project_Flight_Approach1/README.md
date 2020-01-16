## PLEASE  READ BELOW 
### I am Running these jobs on Windows 10 machine , so the jobs are written accordingly

#### Following's are required to run these  job :-
1) Kafka and ZooKeeper installed and configured ( This set up and configuration can be easily get from google results), Make sure zookeeper runs at 2181 and kafka at 9092 port.
2) Pycharm ( To write these jobs and can also be run from editor to see our results ).

### As you can see there are 4 python files , i will explain them one by one.But First follow the steps.
1) Start you zookeeper followed by starting kafka server followed by creating 2 topics ('transactions' and 'locations')
2) Then open Pycharm and load these python files .
3) Extract the Extract_2_json_Files.rar file and place the json files to some new location , this will be your source path.
4) Modify  kafka_producer_locations.py and kafka_producer_transactions.py and change the input file location to your file location,
   Do the same for pyspark_consumer.py for checkpoint locations.
   
#### Purpose of each job(Please check Python Library of each job)
1) kafka_producer_locations.py and kafka_producer_transactions.py are basically same , it will load indiviual json file and publish data to kafka broker for streaming through topics we created for them.
2) Run kafka_producer_locations.py and kafka_producer_transactions.py individually , now the data are getting streamed and avialabe at localhost:9092 via respective topics.
3) After Step2 Run pyspark_consumer.py , This is basically SPARK STRUCTURED STREAMING job as a consumer which will pick those streamed data and load it in 2 tables (FLIGHT_TRANSACTIONS and FLIGHT_LOCATIONS ).
4) Spark will recieve data in string form of dictonary , which we have to expand it and formulate it into structured data.After apply necessary transformations which included flattening of nested json coming in transactions data.
5) At the end the final 2 table structure will look like below as shown, take this data on notepad++ to get good visual  :-


### FLIGHT_TRANSACTIONS:-
+----------------------+-------------------+--------------+-----------------+---------------------------+------------------------------------+--------------------+------------------+---------+------------------+-------------+
|DestinationAirportCode|Itinerary          |OneWayOrReturn|OriginAirportCode|TransactionDateUTC         |UniqueId                            |DepartureAirportCode|ArrivalAirportCode|LegNumber|NumberOfPassengers|SegmentNumber|
+----------------------+-------------------+--------------+-----------------+---------------------------+------------------------------------+--------------------+------------------+---------+------------------+-------------+
|FRA                   |CMB-DOH-FRA-DOH-CMB|Return        |CMB              |2019-12-27 10:38:10.767 UTC|7d880df5-9428-ea11-a822-e453a8e76766|FRA                 |DOH               |1        |1                 |2            |
|FRA                   |CMB-DOH-FRA-DOH-CMB|Return        |CMB              |2019-12-27 10:38:10.767 UTC|7d880df5-9428-ea11-a822-e453a8e76766|DOH                 |CMB               |2        |1                 |2            |
|FRA                   |CMB-DOH-FRA-DOH-CMB|Return        |CMB              |2019-12-27 10:38:10.767 UTC|7d880df5-9428-ea11-a822-e453a8e76766|CMB                 |DOH               |1        |1                 |1            |
|CRK                   |FRA-DOH-CRK-DOH-FRA|Return        |FRA              |2019-11-29 14:02:48.877 UTC|245729ec-b012-ea11-a822-e453a8e76766|FRA                 |DOH               |1        |1                 |1            |
|CRK                   |FRA-DOH-CRK-DOH-FRA|Return        |FRA              |2019-11-29 14:02:48.877 UTC|245729ec-b012-ea11-a822-e453a8e76766|CRK                 |DOH               |1        |1                 |2            |
|CRK                   |FRA-DOH-CRK-DOH-FRA|Return        |FRA              |2019-11-29 14:02:48.877 UTC|245729ec-b012-ea11-a822-e453a8e76766|DOH                 |CRK               |2        |1                 |1            |
|CRK                   |FRA-DOH-CRK-DOH-FRA|Return        |FRA              |2019-11-29 14:02:48.877 UTC|245729ec-b012-ea11-a822-e453a8e76766|DOH                 |FRA               |2        |1                 |2            |
|CNX                   |BKK-CNX            |One Way       |BKK              |2019-01-02 22:42:07.29 UTC |aa02f59c-df0e-e911-a820-a7f2c9e35195|BKK                 |CNX               |1        |1                 |1            |
|AMS                   |ZRH-AMS            |One Way       |ZRH              |2019-02-15 19:05:51.937 UTC|35e004b2-5431-e911-a820-a7f2c9e35195|ZRH                 |AMS               |1        |2                 |1            |
|BKK                   |VIE-BKK-VIE        |Return        |VIE              |2019-06-10 18:46:28.563 UTC|86bbe30d-b08b-e911-a821-8111ee911e19|BKK                 |VIE               |1        |2                 |2            |
+----------------------+-------------------+--------------+-----------------+---------------------------+------------------------------------+--------------------+------------------+---------+------------------+-------------+


### FLIGHT_LOCATIONS
+-----------+-----------+--------+
|AirportCode|CountryName|Region  |
+-----------+-----------+--------+
|JJD        |Brazil     |Americas|
|YYG        |Canada     |Americas|
|XDS        |Canada     |Americas|
|ZWS        |Germany    |Europe  |
+-----------+-----------+--------+


6) After spark loads these 2 tables (hive) , then run data_insights.py job , it will run all the sql queries which is required to get some insights from  the data , this can be seen through charts when the job completes.
