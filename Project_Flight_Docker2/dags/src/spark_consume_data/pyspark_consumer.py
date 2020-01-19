from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import sys
from time import sleep


def consume_locations(input_df,checkpoint_path):
    # The Below Section is for Flight Locations transformation and loading
    new_df = input_df.withColumn('new_val', F.regexp_replace(input_df['value'], '\\\\', '')).drop('value')
    new_df = new_df.withColumn('value', F.regexp_replace(new_df['new_val'], '""', "'")).drop('new_val')
    new_df = new_df.withColumn('new_val', F.regexp_replace(new_df['value'], '}n', "}")).drop('value')
    new_df = new_df.withColumn('new_val', F.regexp_replace(new_df['new_val'], "'", ""))
    #new_df = new_df.withColumn('struct_val',F.struct(new_df['new_val'])).drop('new_val')

    location_schema = StructType((StructField("AirportCode",StringType()),
                                  StructField("CountryName",StringType()),
                                  StructField("Region",StringType())))

    json_df = new_df.select(F.from_json(F.col("new_val"), location_schema).alias("value")).selectExpr('value.*')

    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("LocationTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


def consume_transactions(input_df,checkpoint_path):
    # The Below Section is for Flight Transactions transformation and loading
    new_df = input_df.withColumn('new_val', F.regexp_replace(input_df['value'], '\\\\', '')).drop('value')
    new_df = new_df.withColumn('value', F.regexp_replace(new_df['new_val'], '""', "'")).drop('new_val')
    new_df = new_df.withColumn('new_val', F.regexp_replace(new_df['value'], '}n', "}")).drop('value')
    new_df = new_df.withColumn('new_val', F.regexp_replace(new_df['new_val'], "'", ""))
    #new_df = new_df.withColumn('struct_val',F.struct(new_df['new_val'])).drop('new_val')

    transaction_schema = StructType((StructField("DestinationAirportCode", StringType()),
                             StructField("Itinerary", StringType()),
                             StructField("OneWayOrReturn", StringType()),
                             StructField("OriginAirportCode", StringType()),
                             StructField("Segment", ArrayType(StructType([StructField("ArrivalAirportCode", StringType()),
                                                                   StructField("DepartureAirportCode", StringType()),
                                                                   StructField("LegNumber", StringType()),
                                                                   StructField("NumberOfPassengers", StringType()),
                                                                   StructField("SegmentNumber", StringType())]))),
                    StructField("TransactionDateUTC", StringType()),
                    StructField("UniqueId", StringType())))

    json_df = new_df.select(F.from_json(F.col("new_val"), transaction_schema).alias("value")).selectExpr('value.*')
    json_df = json_df.withColumn('Segment', F.explode_outer(json_df['Segment']))

    json_df = json_df.withColumn('DepartureAirportCode', F.col('Segment')['DepartureAirportCode']) \
        .withColumn('ArrivalAirportCode', F.col('Segment')['ArrivalAirportCode']) \
        .withColumn('LegNumber', F.col('Segment')['LegNumber']) \
        .withColumn('NumberOfPassengers', F.col('Segment')['NumberOfPassengers']) \
        .withColumn('SegmentNumber', F.col('Segment')['SegmentNumber']) \
        .drop('Segment')

    # Stream the data, from a Kafka topic to a Spark in-memory table
    query = json_df\
        .writeStream \
        .format("memory") \
        .queryName("TransactionTable") \
        .outputMode("append") \
        .option("checkpoint",checkpoint_path)\
        .start()

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


def pyspark_consumer(checkpoint_trans_path,checkpoint_loc_path,CLIENT,TOPICS,spark):

    spark = spark
    checkpoint_trans_path = checkpoint_trans_path
    checkpoint_loc_path = checkpoint_loc_path
    client = CLIENT
    topics = TOPICS

    trans_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", client) \
        .option("subscribe", topics) \
        .option("startingOffsets", "earliest") \
        .load()

    matchdata_locations = trans_df.selectExpr("CAST(value AS STRING) as value").filter("topic = 'locations'")
    matchdata_transactions = trans_df.selectExpr("CAST(value AS STRING) as value").filter("topic = 'transactions'")

    # Call transaction function
    consume_transactions(matchdata_transactions,checkpoint_trans_path)

    # Call locations function
    consume_locations(matchdata_locations, checkpoint_loc_path)

    # Check if the dataframe is still streaming , if not then get out
    while trans_df.isStreaming:
        trans_df = spark.sql("select * from TransactionTable")
        loc_df = spark.sql("select * from LocationTable")
        #trans_df.show(10,False)
        #loc_df.show(10,False)
        trans_df.write.saveAsTable("FLIGHT_TRANSACTIONS",mode="overwrite")
        loc_df.write.saveAsTable("FLIGHT_LOCATIONS", mode="overwrite")
        sleep(5)


def main():

    checkpoint_trans_path = sys.argv[1]
    checkpoint_loc_path = sys.argv[2]
    CLIENT = sys.argv[3]
    TOPICS = sys.argv[4]

    spark = SparkSession \
        .builder \
        .appName("Pyspark_consumer") \
        .enableHiveSupport()\
        .config("kafka.partition.assignment.strategy", "range") \
        .getOrCreate()

    pyspark_consumer(checkpoint_trans_path,checkpoint_loc_path,CLIENT,TOPICS,spark)


if __name__ == "__main__":
    main()
