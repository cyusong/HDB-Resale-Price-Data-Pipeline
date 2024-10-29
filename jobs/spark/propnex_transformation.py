import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def get_spark_session(app_name):
    return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

def clean_agent_info(df):
    return df.withColumn('agent_email', F.lower(col('agent_email'))) \
             .withColumn('agent_id', F.substring_index(col('agent_id'), '#', -1)) \
             .withColumn('agent_phone_num', F.substring_index(col('agent_phone_num'), ' ', -1).cast('int'))

def clean_description(df):
    emoji_pattern = '[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F5FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF\U00002700-\U000027BF]+'
    return df.withColumn('description', F.trim(F.regexp_replace(F.regexp_replace(col('description'), emoji_pattern, ''), '\n', ' ')))

def extract_floor_area(df):
    return df.withColumn('floor_area_sqm', F.regexp_extract(col('floor_area_sqft'), r'\((\d+)\s*sqm\)', 1))

def clean_price_and_calculate_price_psf(df):
    return df.withColumn('price', F.regexp_replace(col('price'), r'[$,]', '').cast('int')) \
             .withColumn('price_psf', F.round(col('price') / col('total_floor_area')).cast('int'))

def process_furnishing_info(df):
    return df.withColumn('furnish', F.when(F.lower(col('furnishing')) == 'partially furnished', 'partial')
                                     .when(F.lower(col('furnishing')) == 'fully furnished', 'full')
                                     .when(F.lower(col('furnishing')) == 'unfurnished', 'unfurnished')
                                     .otherwise(col('furnishing')))

def extract_general_location(df):
    return df.withColumn('general_location', F.split(F.split(col('street_town_district'), '\\n').getItem(1), ' \\(').getItem(0)) \
             .withColumn('general_location', F.when(col('general_location').startswith('(D'), None)
                                              .otherwise(col('general_location')))

def calculate_facilities(df):
    return df.withColumn('facilities_num', F.when(col('facilities').isNull(), None)
                                            .otherwise(F.size(F.split(col('facilities'), ',')))) 

def transform_location_col(df):
    return df.withColumn('location', F.initcap(col('location'))) \
             .withColumn('location', F.regexp_replace(col('location'), r'^(Blk\s+|Block\s+)', '')) \
             .withColumn('location_1', F.split(col('location'), ' ').getItem(0)) \
             .withColumn('location_2', F.concat_ws(' ', F.slice(F.split(col('location'), ' '), 2, F.size(F.split(col('location'), ' '))))) \
             .withColumn('location', F.concat_ws(' ' \
                                                , F.when(col('location_1').rlike(r'^\d'), F.upper(col('location_1'))) \
                                                   .otherwise(col('location_1')) \
                                                , 'location_2')) \
             .withColumn('location', F.when(F.trim(col('location')).rlike(r'^\d+$'), F.initcap(col('street_name')))
                                      .otherwise(col('location')))

def transform_others(df):
    return df.withColumn('district', F.substring_index(col('district'), 'D', -1).cast('int')) \
             .withColumn('floor', F.lower(F.split(col('floor'), ' ').getItem(0))) \
             .withColumn('street_name', F.initcap(col('street_name')))

def rename_col(df):
    return df.withColumnRenamed('num_bedroom', 'bedrooms') \
             .withColumnRenamed('num_bathroom', 'bathrooms')

def assign_agency_name_id(df):
    return df.withColumn('agency', F.when(col('agent_email').contains('propnex.com'), 'PROPNEX REALTY PTE. LTD.')
                                    .otherwise(None)) \
             .withColumn('agency_id', F.when(col('agency') == 'PROPNEX REALTY PTE. LTD.', 'L3008022J')
                                       .otherwise(None))

def calculate_remaining_lease(df):
    return df.withColumn('remaining_lease', (99 - (datetime.datetime.today().year - col('top')).cast('int')))

def create_postal_sector(df):
    return df.withColumn('postal_sector', F.substring(F.col('post_code'), 1, 2))

def broadcast_join(df, small_df, column, method):
    return df.join(F.broadcast(small_df), on=column, how=method)

def create_additional_info(df):
    return df.withColumn(
            'additional_information',
                F.to_json(
                    F.struct(
                        F.coalesce(F.col('agent_email'), F.lit('N/A')).alias('agent_email'),
                        F.coalesce(F.col('facilities'), F.lit('N/A')).alias('facilities'),
                        F.coalesce(F.col('description'), F.lit('N/A')).alias('description'),
                    )
                )
            )

def create_none_col(df):
    return df.withColumn('property_type', F.lit(None).cast('string')) \
             .withColumn('model', F.lit(None).cast('string'))

def type_casting(df, to_be_casted_columns):
    for column, dtype in to_be_casted_columns.items():
        df = df.withColumn(column, col(column).cast(dtype))
    return df

def write_to_parquet(df, output_path):
    df.write.mode('overwrite').parquet(output_path)

def main(input_path, output_path):
    # Start Spark session
    spark = get_spark_session('pronex_transformation')

    try:
        # Load data
        df = spark.read.option('multiline', 'true').json(input_path)

        # Clean and transform data
        df = df.na.replace(['None', ''], None)
        df = clean_agent_info(df)
        df = clean_description(df)
        df = extract_floor_area(df)
        df = clean_price_and_calculate_price_psf(df)
        df = process_furnishing_info(df)
        df = extract_general_location(df)
        df = calculate_facilities(df)
        df = transform_location_col(df)
        df = transform_others(df)
        df = assign_agency_name_id(df)
        df = calculate_remaining_lease(df)
        df = create_additional_info(df)
        df = create_postal_sector(df)

        district_code_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_code_table/')
        district_region_df = spark.read.parquet('gs://hdb-resale-price-data-pipeline/data/district_region_table')
        df = broadcast_join(df, district_code_df.select(col('district').alias('district_new') ,'postal_sector', 'zone'), 'postal_sector', 'left')
        df = df.withColumn('district', col('district_new'))
        df = broadcast_join(df, district_region_df, 'district', 'left')
        
        df = create_none_col(df)
        df = rename_col(df)

        df = df.drop('floor_area_sqft', 'listing_type', 'property_group', 'street_town_district', 'tenure', 'location_1', 'location_2', 'furnishing', 'facilities', 'agent_email', 'postal_sector', 'description', 'district_new')
        df = df.na.replace(['None', ''], None)
        
        to_be_casted_columns = {
            'bathrooms' : 'int',
            'bedrooms' : 'int', 
            'top' : 'int',
            'total_floor_area' : 'int',
            'floor_area_sqm' : 'int',
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
        print('Usage: propnex_transformation.py <input_json_path> <output_parquet_path>')
        sys.exit(1)

    input_json_path = sys.argv[1]
    output_parquet_path = sys.argv[2]

    main(input_json_path, output_parquet_path)