import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window


def get_spark_session(app_name):
    return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

def remove_row_with_null(df):
    return df.filter(
        F.col('agent_id').isNotNull() &
        F.col('location').isNotNull() &
        F.col('price').isNotNull())

def deduplication(df):
    df = df.withColumn('null_count',
                       sum(F.when(col(c).isNull(), 1).otherwise(0) for c in df.columns))

    # Keep row with the least number of null value and remove all the duplicated row 
    window_spec = Window.partitionBy('location', 'price').orderBy(F.col('null_count'))
    df = df.withColumn('row_num', F.row_number().over(window_spec))
    df = df.filter(F.col('row_num') == 1).drop('null_count', 'row_num')

    return df

def add_transformation_date(df):
    return df.withColumn('transformed_date', F.lit(datetime.datetime.today().date()))

def reorder_column(df):
    col_order = [
        'location', 'price', 'price_psf', 'top', 'remaining_lease', 'bathrooms', 'bedrooms', 'total_floor_area', 
        'property_type', 'model', 'floor', 'general_location', 'district', 'zone', 'region', 
        'street_name', 'post_code', 'furnish', 'floor_area_sqm', 'facilities_num', 
        'agent_id', 'agent_name', 'agent_phone_num', 'agency', 'agency_id', 'url', 'additional_information', 'transformed_date']

    return df.select(col_order)

def write_to_parquet(df, output_path):
    df.write.mode('overwrite').parquet(output_path)

def main(input_path_one, input_path_two, output_path):
    # Start Spark session
    spark = get_spark_session('merge_dudup')

    try:
        # Load data
        df_one = spark.read.parquet(input_path_one)
        df_two = spark.read.parquet(input_path_two)

        # Union the two df
        df = df_one.unionByName(df_two)

        # Merge and dedup data
        df = remove_row_with_null(df)
        df = deduplication(df)
        df = add_transformation_date(df)
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
        print('Usage: merge_dedup.py <input_parquet_path_one> <input_parquet_path_two> <output_parquet_path>')
        sys.exit(1)

    input_parquet_path_one = sys.argv[1]
    input_parquet_path_two = sys.argv[2]
    output_parquet_path = sys.argv[3]

    main(input_parquet_path_one, input_parquet_path_two, output_parquet_path)