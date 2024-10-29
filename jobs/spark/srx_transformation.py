import sys
import re 
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

def get_spark_session(app_name):
    return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

def clean_agent_info(df):
    return df.withColumn('agency_id', F.regexp_extract(col('agent_id'), r'(L\d{7}[A-Z])', 1)) \
             .withColumn('agent_id', F.regexp_extract(col('agent_id'), r'(R\d{6}[A-Z])', 1)) \
             .withColumn('agent_phone_num', F.split(col('agent_phone_num'), ':').getItem(1).cast('int')) \
             .withColumn('agent_name', F.trim(F.regexp_replace(col('agent_name'), r'[^\x00-\x7F]+', '')))

def clean_address_and_extract_post_code(df):
    return df.withColumn('post_code', F.regexp_extract(col('address'), r'\((\d+)\)', 1)) \
             .withColumn('location', F.split(col('address'), ' \\(').getItem(0))

def transform_location_col(df):
    return df.withColumn('location', F.initcap(col('location'))) \
             .withColumn('location', F.regexp_replace(col('location'), r'^(Blk\s+|Block\s+)', '')) \
             .withColumn('location_1', F.split(col('location'), ' ').getItem(0)) \
             .withColumn('location_2', F.concat_ws(' ', F.slice(F.split(col('location'), ' '), 2, F.size(F.split(col('location'), ' '))))) \
             .withColumn('location', F.concat_ws(' ' \
                                                , F.when(col('location_1').rlike(r'^\d'), F.upper(col('location_1'))) \
                                                   .otherwise(col('location_1')) \
                                                , 'location_2'))

def clean_description(df):
    emoji_pattern = '[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF\U00002700-\U000027BF]+'
    return df.withColumn('description', F.trim(F.regexp_replace(F.regexp_replace(col('description'), emoji_pattern, ''), '\n', ' '))) 

def calculate_facilities(df):
    return df.withColumn('facilities_num', F.when(col('facilities').isNull(), None)
                                            .otherwise(F.size(F.split(col('facilities'), ','))))

def process_furnishing_info(df):
    return df.withColumn('furnish', F.when(F.lower(col('furnish')) == 'partially furnished', 'partial')
                                     .when(F.lower(col('furnish')) == 'fully furnished', 'full')
                                     .when(F.lower(col('furnish')) == 'not furnished', 'unfurnished')
                                     .otherwise(col('furnish')))

def clean_price_and_price_psf(df):
    return df.withColumn('price', F.regexp_replace(col('price'), r'[$,]', '').cast('int')) \
             .withColumn('price_psf', F.regexp_replace(F.regexp_extract(F.col('psf'), r'\$?([\d,]+)\s*psf', 1), ',', '').cast('int'))

def calculate_floor_area_sqft_sqm(df):
    conversion_factor = 3.28084 * 3.28084
    return df.withColumn('floor_area_sqm', F.trim(F.split(col('size'), 'sqm').getItem(0)).cast('int')) \
             .withColumn('total_floor_area', F.round(col('floor_area_sqm') * conversion_factor).cast('int'))

def transform_others(df):
    return df.withColumn('property_type', F.trim(F.split(col('property_type'), 'HDB').getItem(1))) \
             .withColumn('floor', F.lower(col('floor_level'))) \
             .withColumn('floor', F.when(col('floor')=='mid','middle') \
                                   .otherwise(col('floor')))

def rename_col(df):
    return df.withColumnRenamed('built_year', 'top') \
             .withColumnRenamed('property_name', 'street_name') \
             .withColumn('street_name', F.initcap(col('street_name'))) \
             .withColumnRenamed('hdb_town', 'general_location')

def clean_bedrooms_udf(value):
    if value is None:
        return 'None'
    if value.lower() == 'studio':
        return 1  # Assume 'Studio' means 1 bedroom
    # Use regex to check for expressions like '3+1'
    if re.match(r'^\d+\+\d+$', value):
        return sum(map(int, value.split('+')))  # Evaluate '3+1' as 4
    try:
        return int(value)  # Cast plain numbers directly to int type
    except ValueError:
        return 'None'

def clean_bedrooms(df):
    bedrooms_udf = F.udf(clean_bedrooms_udf)
    return df.withColumn('bedrooms', bedrooms_udf(col('bedrooms')).cast('int'))

def calculate_remaining_lease(df):
    return df.withColumn('remaining_lease', (99 - (datetime.datetime.today().year - col('top')).cast('int')))

def create_postal_sector(df):
    return df.withColumn('postal_sector', F.substring(F.col('post_code'),1, 2))

def broadcast_join(df, small_df, column, method):
    return df.join(F.broadcast(small_df), on=column, how=method)

def create_additional_info(df):
    return df.withColumn(
                'additional_information',
                F.to_json(
                    F.struct(
                        F.coalesce(F.col('facilities'), F.lit('N/A')).alias('facilities'),
                        F.coalesce(F.col('schools'), F.lit('N/A')).alias('schools'),
                        F.coalesce(F.col('shopping_mall/markets'), F.lit('N/A')).alias('shopping_malls/markets'),
                        F.coalesce(F.col('train_stations'), F.lit('N/A')).alias('train_stations'),
                        F.coalesce(F.col('description'), F.lit('N/A')).alias('description')
                    )
                )
            )

def type_casting(df, to_be_casted_columns):
    for column, dtype in to_be_casted_columns.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df

def write_to_parquet(df, output_path):
    df.write.mode('overwrite').parquet(output_path)

def main(input_path, output_path):
    # Start Spark session
    spark = get_spark_session('srx_transformation')

    try:
        # Load data
        df = spark.read.option('multiline', 'true').json(input_path)

        # Clean and transform data
        df = df.na.replace(['None', ''], None)
        df = clean_agent_info(df)
        df = clean_address_and_extract_post_code(df)
        df = transform_location_col(df)
        df = clean_description(df)
        df = calculate_facilities(df)
        df = process_furnishing_info(df)
        df = clean_price_and_price_psf(df)
        df = calculate_floor_area_sqft_sqm(df)
        df = transform_others(df)
        df = rename_col(df)
        df = clean_bedrooms(df)
        df = calculate_remaining_lease(df)
        df = create_postal_sector(df)
        df = create_additional_info(df)


        district_code_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_code_table/')
        district_region_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_region_table')
        agency_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/agency_id')

        df = broadcast_join(df, district_code_df, 'postal_sector', 'left')
        df = broadcast_join(df, district_region_df, 'district', 'left')
        df = broadcast_join(df, agency_df, 'agency_id', 'left')

        df = df.drop('asking', 'date_listed', 'developer', 'tenancy_status', 'tenure', 'psf', 'floor_level', 'floor_size_psf', 'address', 'size', 'location_1', 'location_2', 'num_bedroom', 'num_bathroom', 'facilities', 'schools', 'shopping_mall/markets', 'train_stations', 'postal_sector', 'description')
        df = df.na.replace(['None', ''], None)
        
        to_be_casted_columns = {
            'bathrooms' : 'int', 
            'top' : 'int',
        }
        df = type_casting(df, to_be_casted_columns)
        
        # Export cleaned data to Parquet
        write_to_parquet(df, output_path)

    except Exception as e:
        sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: srx_transformation.py <input_json_path> <output_parquet_path>')
        sys.exit(1)

    input_json_path = sys.argv[1]
    output_parquet_path = sys.argv[2]

    main(input_json_path, output_parquet_path)