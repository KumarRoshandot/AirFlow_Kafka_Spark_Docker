## This is Same as Folder Project_Flight with one less container in docker-compose

### Here We will have only 4 container instead of 5.
#### Spark and Airflow will reside in same container

#### Now for this Airflow Dag has been changed for JOB2 and JOB3 
Instead of using DockerOperator , here BashOperator is been used to call spark-submit on the same server and this is the only change ,Rest all woll be same as Approach1 (Project_Flight)
