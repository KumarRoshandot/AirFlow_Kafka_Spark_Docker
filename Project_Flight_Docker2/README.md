## This is Same as Folder Project_Flight with one less container in docker-compose
## This is Approach3 again using docker-compose

### Here We will have only 4 container instead of 5.
#### Spark and Airflow will reside in same container

#### Now for this Airflow Dag has been changed for JOB2 and JOB3 
1) Instead of using DockerOperator , here BashOperator is been used to call spark-submit on the same server and this is the only change ,Rest all will be same as Approach1 (Project_Flight).
2) Here I have to Work on DockerFile to combine airflow , spark and hadoop , Luckly i had airflow image avaiable on docker-hub , i just had to build spark and hadoop on top of it ( Check out the dockerfile kept in airflow_docker folder ).

### Couple of challenges when u run a spark-submit and that to for  streaming kafka jobs :-
1) Play carefully when u pass parameters to python job , and check how do you pass it through DAG file ( Check code )
2) When it comes to spark-streaming , you need to be carefull with the jars which are required to run spark-streaming-kafka jobs ( Check code what all jars were passed in the DAG )
3) We can definetly use BashOperator for spark-submit as we require it to run on shell , but Airflow gives another SparkSubmitOperator which also can be use to trigger spark-submit jobs.(read documentation for it ).

### Kafka Client Jar issue 
1) There was one error which kept on coming while i was running spark-streaming consumer job from airflow, issue was 'pyspark.sql.utils.StreamingQueryException: 'Missing required configuration "partition.assignment.strategy" which has no default value'.
2) So to Fix this issue i had to place kafka-clients-1.1.0.jar in docker folder and set instructions in dockerfile to get copied to SPARK_HOME/jars Folder .
3) I had to specify '--conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar" ' while passing spark-submit .
and walaaa it ran successfully ...:)

### HIve Important Settings Before triggering DAGs.
PLease read HIve_important_settings.txt

