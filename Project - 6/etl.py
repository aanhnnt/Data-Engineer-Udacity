import pandas as pd
import glob
import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType, TimestampType    

# Setup Logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_spark_session():
    spark = SparkSession.builder\
                        .config('spark.jars.repositories', 'https://repos.spark-packages.org/') \
                        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11') \
                        .enableHiveSupport()\
                        .getOrCreate()
    return spark

@udf(TimestampType())
def to_timestamp_udf(x):
    try:
        return pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1')
    except:
        return pd.Timestamp('1900-1-1')

def process_immigration_data(spark, input_data, output_data):
    logging.info('Start processing immigration data')
    
    # Read immigration data file to dataframe
    df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    
    # Change data type of some columns from double to integer
    toInt = udf(lambda x: int(x) if x!=None else x, IntegerType())

    for colname, coltype in df.dtypes:
        if coltype == 'double':
            df = df.withColumn(colname, toInt(colname))
            
    logging.info('Start processing fact_immigration table')
    
    # Extract columns to create fact_immigration table
    fact_immigration_df = df.select('cicid', 'i94port', 'i94addr', 'i94visa', 'i94yr', \
                                    'i94mon', 'i94mode', 'arrdate', 'depdate').distinct()

    # Rename columns of fact_immigration table
    fact_immigration_df = fact_immigration_df.withColumnRenamed('i94port', 'city_code') \
                                    .withColumnRenamed('i94addr', 'state_code') \
                                    .withColumnRenamed('i94visa', 'visa') \
                                    .withColumnRenamed('i94yr', 'year') \
                                    .withColumnRenamed('i94mon', 'month') \
                                    .withColumnRenamed('i94mode', 'mode') \
                                    .withColumnRenamed('arrdate', 'arrive_date') \
                                    .withColumnRenamed('depdate', 'departure_date')

    # Drop null records on state_code column
    fact_immigration_df = fact_immigration_df.where(col('state_code').isNotNull())

    # Add country column to fact_immigration table
    fact_immigration_df = fact_immigration_df.withColumn('country', lit('United States'))

    # Change date type from SAS to timestamp
    fact_immigration_df = fact_immigration_df.withColumn('arrive_date', to_date(to_timestamp_udf(col('arrive_date')))) \
                                    .withColumn('departure_date', to_date(to_timestamp_udf(col('departure_date'))))                                    

    # Write fact_immigration table to parquet files and partition by state_code
    fact_immigration_df.write.mode('overwrite') \
            .partitionBy('state_code') \
            .parquet(output_data + 'fact_immigration')

    # Create view for quality check
    fact_immigration_df.createOrReplaceTempView('fact_immigration')
    
    logging.info('Finish processing fact_immigration table')

    logging.info('---------------------------------------')

    logging.info('Start processing dim_immigrate_person table')

    # Extract columns to create dim_immigrate_person
    dim_immigrate_person_df = df.select('cicid', 'i94cit', 'i94res',\
                                    'biryear', 'gender', 'insnum').distinct()
    
    # Rename columns of dim_immigrate_person table
    dim_immigrate_person_df = dim_immigrate_person_df.withColumnRenamed('i94cit', 'citizen_country_code') \
                                    .withColumnRenamed('i94res', 'citizen_state_code') \
                                    .withColumnRenamed('biryear', 'birth_year')

    # Write dim_immigrat_person table to parquet files    
    dim_immigrate_person_df.write.mode('overwrite') \
            .parquet(output_data + 'dim_immigrate_person')

    # Create view for quality check
    dim_immigrate_person_df.createOrReplaceTempView('dim_immigrate_person')

    logging.info('Finish processing dim_immigrate_person table')

    logging.info('---------------------------------------')

def process_temperature_data(spark, input_data, output_data):
    logging.info('Start processing dim_temperature table')

    # Read temperature data file to dataframe
    df = spark.read.csv(input_data, header=True)

    # Filter data in United States
    df = df.where(df['Country'] == 'United States')

    # Extract columns to create dim_temperature table
    dim_temperature_df = df.select('dt', 'AverageTemperature', 'AverageTemperatureUncertainty', \
                            'City', 'Country').distinct()

    # Rename columns of dim_temperature
    dim_temperature_df = dim_temperature_df.withColumnRenamed('AverageTemperature', 'avg_temperture') \
                                .withColumnRenamed('AverageTemperatureUncertainty', 'avg_temp_uncertainty') \
                                .withColumnRenamed('City', 'city') \
                                .withColumnRenamed('Country', 'country')

    # Extract year, month from dt column
    dim_temperature_df = dim_temperature_df.withColumn('dt', to_date(col('dt')))
    dim_temperature_df = dim_temperature_df.withColumn('year', year(dim_temperature_df['dt']))
    dim_temperature_df = dim_temperature_df.withColumn('month', month(dim_temperature_df['dt']))

    # Write dim_temperature to parquet files
    dim_temperature_df.write.mode('overwrite') \
            .parquet(output_data + 'dim_temperature')

    # Create view for quality check
    dim_temperature_df.createOrReplaceTempView('dim_temperature')

    logging.info('Finish processing dim_temperature table')

    logging.info('---------------------------------------')

def process_demographic_data(spark, input_data, output_data):
    logging.info('Start processing dim_demographic table')

    # Read demographic data to dataframe
    df = spark.read.format('csv').options(header=True, delimiter=';').load(input_data)
    
    # Rename columns of dim_demographic table
    dim_demographic_df  = df.withColumnRenamed('City','city') \
                                    .withColumnRenamed('State','state') \
                                    .withColumnRenamed('Median Age','median_age') \
                                    .withColumnRenamed('Male Population','male_population') \
                                    .withColumnRenamed('Female Population','female_population') \
                                    .withColumnRenamed('Total Population','total_population') \
                                    .withColumnRenamed('Number of Veterans','number_of_veterans') \
                                    .withColumnRenamed('Foreign-born','foreign_born') \
                                    .withColumnRenamed('Average Household Size','avg_household_size') \
                                    .withColumnRenamed('State Code','state_code')\
                                    .withColumnRenamed('Race','race') \
                                    .withColumnRenamed('Count','count')
    
    # Change type of some columns to float
    colnames = ['median_age', 'male_population', 'female_population', 'total_population', 
                'number_of_veterans', 'foreign_born', 'avg_household_size', 'count']
    
    toFloat = udf(lambda x: float(x) if x!=None else x, FloatType())
    
    for colname in colnames:
        dim_demographic_df = dim_demographic_df.withColumn(colname, toFloat(colname))

    # Write dim_demographic table to parquet files
    dim_demographic_df.write.mode('overwrite') \
            .parquet(output_data + 'dim_demographic')
    
    # Create view for quality check
    dim_demographic_df.createOrReplaceTempView('dim_demographic')

    logging.info('Finish processing dim_demographic table')

    logging.info('---------------------------------------')

def process_airport_data(spark, input_data, output_data): 
    logging.info('Start processing dim_airport table')

    # Read airport data to dataframe
    df = spark.read.csv(input_data, header=True)
    
    # Convert elevation_ft to type integer
    df = df.withColumn('elevation_ft', col('elevation_ft').cast('integer'))
   
    # Add state_code column to join with fact table
    df = df.withColumn('state_code', split(col('iso_region'), '-').getItem(1)) 
    
    # Extract columns to create dim_airport table
    dim_airport_df = df.select('ident', 'type', 'name', 'elevation_ft','iso_country', \
                                'state_code', 'municipality', 'coordinates')

    # Write dim_airport table to parquet files
    dim_airport_df.write.mode('overwrite')\
            .parquet(output_data + 'dim_airport')
    
    # Create view for quality check
    dim_airport_df.createOrReplaceTempView('dim_airport')

    logging.info('Finish processing dim_airport table')

    logging.info('---------------------------------------')

def check_data_quality(spark):
    logging.info('Start checking data quality')

    tables = ['fact_immigration', 'dim_immigrate_person', 'dim_temperature', 'dim_demographic', 'dim_airport']

    for table in tables:
        print(f'Checking data quality on table {table}...')
        expected_result = spark.sql(f'''SELECT COUNT(*) FROM {table}''')
        if expected_result.head()[0] > 0:
            print(f'Table {table} passed. {expected_result.head()[0]} records')
        else:
            print(f'Table {table}. Data quality check failed!')

    logging.info('Finish checking data quality')

    logging.info('---------------------------------------')      

def main():
    spark = create_spark_session()
    output_data = './output_data/'

    immigration_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    temperature_data = '../../data2/GlobalLandTemperaturesByCity.csv'
    demographic_data = './us-cities-demographics.csv'
    airport_data = './airport-codes_csv.csv'

    process_immigration_data(spark, immigration_data, output_data)
    process_temperature_data(spark, temperature_data, output_data)
    process_demographic_data(spark, demographic_data, output_data)
    process_airport_data(spark, airport_data, output_data)

    spark.stop()