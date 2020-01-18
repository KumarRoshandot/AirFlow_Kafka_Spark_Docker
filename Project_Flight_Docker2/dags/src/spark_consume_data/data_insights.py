from pyspark.sql import SparkSession
from matplotlib import pyplot


def max_originating_trans(spark):
    spark = spark
    sql = '''
    SELECT 
    loc.CountryName as country,
    count(distinct trans.UniqueId) as total_transactions 
    FROM 
    FLIGHT_TRANSACTIONS trans JOIN FLIGHT_LOCATIONS loc
    on trans.OriginAirportCode = loc.AirportCode
    group by loc.CountryName
    '''
    df = spark.sql(sql)
    df_data = df.collect()

    # create a numeric value for every label
    indexes = list(range(len(df_data)))

    # split the data into diff lists
    values = [row['total_transactions'] for row in df_data]
    labels = [row['country'] for row in df_data]

    # Plotting
    bar_width = 0.35

    # histogram
    pyplot.bar(indexes,values)

    # add labels
    labelidx = [i + bar_width for i in indexes]
    pyplot.xticks(labelidx,labels)
    pyplot.show()
    #pdf = df.toPandas()
    #pdf.plot(kind='barh',x='total_transactions',y='country',colormap='winter_r')


def split_domestic_international(spark):
    spark = spark
    sql = '''
    SELECT 
    sum(case when loc_dept.Region = loc_arival.Region then 1 else 0 end) as domestic_flights,
    sum(case when loc_dept.Region <> loc_arival.Region then 1 else 0 end) as international_flights
    FROM 
    FLIGHT_TRANSACTIONS trans JOIN FLIGHT_LOCATIONS loc_dept
    on trans.DepartureAirportCode = loc_dept.AirportCode
    JOIN FLIGHT_LOCATIONS loc_arival
    on trans.ArrivalAirportCode = loc_arival.AirportCode
    '''
    df_splits = spark.sql(sql)
    df_data = df_splits.collect()

    # split the data into diff lists
    values = [row['domestic_flights'] for row in df_data] + [row['international_flights'] for row in df_data]
    labels = df_splits.schema.names
    colors = ['r','g']

    # Pie chart
    pyplot.pie(values,labels=labels,colors=colors,startangle=90,autopct='%.1f%%')
    pyplot.show()


def distribution_segments(spark):
    spark = spark
    sql = '''
    SELECT 
    UniqueId,
    SegmentNumber,
    count(1) as segment_count 
    FROM FLIGHT_TRANSACTIONS
    group by 
    UniqueId,
    SegmentNumber
    '''
    df_dist_segment = spark.sql(sql)
    df_dist_segment.show(20,False)


def main():
    spark = SparkSession \
        .builder \
        .appName("FLIGHT_INSIGHTS") \
        .enableHiveSupport()\
        .getOrCreate()

    max_originating_trans(spark)

    split_domestic_international(spark)

    distribution_segments(spark)


if __name__ == "__main__":
    main()
