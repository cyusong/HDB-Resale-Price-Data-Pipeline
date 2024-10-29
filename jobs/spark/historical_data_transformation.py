import sys
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def get_spark_session(app_name):
    return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

def load_data(bucket_name, spark_session, input_gcs_dir):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_gcs_dir)
    df = None
    for blob in blobs:
        if blob.name.endswith('.csv'):
            path = f'gs://{bucket_name}/{blob.name}'
            temp = spark_session.read.csv(path, header=True)
            temp = temp.select('month', 'town', 'flat_type', 'block', 'street_name', 'storey_range', 'floor_area_sqm', 'flat_model', 'lease_commence_date', 'resale_price')
            if df is None:
                df = temp
            else:
                df = df.unionByName(temp)
    return df

def capitalize_col(df):
    return df.withColumn('general_location', F.initcap(col('town'))) \
             .withColumn('street_name', F.initcap(col('street_name'))) \
             .withColumn('property_type', F.initcap(col('flat_type')))

def rename_col(df):
    return df.withColumnRenamed('flat_model', 'model') \
             .withColumnRenamed('resale_price', 'price') \
             .withColumnRenamed('lease_commence_date', 'top')


def clean_property_type(df):
    return df.withColumn('property_type', F.regexp_replace(col('property_type'), 'Room', 'Rooms')) \
             .withColumn('property_type', F.regexp_replace(col('property_type'), 'Multi G', 'Multi-g'))

def calculate_floor_area_sqft_sqm(df):
    conversion_factor = 3.28084 * 3.28084
    return df.withColumn('total_floor_area', F.round(col('floor_area_sqm') * conversion_factor).cast('int'))

def process_date_col(df):
    return df.withColumn('date_of_sale', F.to_date(F.concat_ws('-', col('month'), F.lit('01')), 'yyyy-MM-dd')) \
             .withColumn('year', F.year('date_of_sale')) \
             .withColumn('month', F.month('date_of_sale'))

def calculate_remaining_lease(df):
    return df.withColumn('remaining_lease', (99 - (col('year') - col('top')).cast('int')))

def broadcast_join(df, small_df, column, method):
    return df.join(F.broadcast(small_df), on=column, how=method)

def assign_district_value_based_on_town(df, town_to_district_df):
    df = df.withColumn('general_location', F.lower('general_location'))
    df = broadcast_join(df, town_to_district_df.withColumn('general_location', F.lower('general_location')), 'general_location', 'left')
    return df.withColumn('general_location', F.initcap('general_location')) \
             .withColumn('general_location', F.regexp_replace('general_location', r'/w', '/W'))


def type_casting(df, to_be_casted_columns):
    for column, dtype in to_be_casted_columns.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df

def reorder_column(df):
    col_order = [
        'date_of_sale', 'year', 'month', 'general_location', 'block', 
        'street_name', 'price', 'top', 'remaining_lease', 'storey_range', 
        'total_floor_area', 'floor_area_sqm', 'model', 'property_type', 'district', 'zone', 'region']

    return df.select(col_order)

def write_to_parquet(df, output_path):
    df.write.mode('overwrite').parquet(output_path)

def main(bucket_name, input_gcs_dir, output_path):
    # Start Spark session
    spark = get_spark_session('historical_data_transformation')

    try:
        # Load data
        df = load_data(bucket_name, spark, input_gcs_dir)

        # Clean and transform data
        df = capitalize_col(df)
        df = rename_col(df)
        df = clean_property_type(df)
        df = calculate_floor_area_sqft_sqm(df)
        df = process_date_col(df)
        df = calculate_remaining_lease(df)

        district_code_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_code_table/')
        district_region_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_region_table')
        town_district_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/town_district_table')

        df = assign_district_value_based_on_town(df, town_district_df)
        df = broadcast_join(df, district_code_df.select('district', 'zone').distinct(), 'district', 'left')
        df = broadcast_join(df, district_region_df, 'district', 'left')
    
        df = df.drop('town', 'flat_type')
        
        to_be_casted_columns = {
            'district' : 'int',
            'floor_area_sqm' : 'int', 
            'top' : 'int',
            'price' : 'int',
            'floor_area_sqm' : 'int',
        }
        df = type_casting(df, to_be_casted_columns)
        df = reorder_column(df)

        # Export cleaned data to Parquet
        write_to_parquet(df, output_path)

    except Exception as e:
        sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: historical_data_transformation.py <bucket_name> <input_gcs_dir> <output_parquet_path>')
        sys.exit(1)

    bucket_name = sys.argv[1]
    input_gcs_dir = sys.argv[2]
    output_parquet_path = sys.argv[3]

    main(bucket_name, input_gcs_dir, output_parquet_path)