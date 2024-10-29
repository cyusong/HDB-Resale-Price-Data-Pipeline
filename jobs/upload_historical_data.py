from google.cloud import storage
import os
def upload_to_gcs(bucket_name, prefix, file_list):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
 
    for file_path in file_list:
        base_name = os.path.basename(file_path)
        destination = f'{prefix}{base_name}'
        blob = bucket.blob(destination)
        blob.upload_from_filename(file_path)
        print(f'Uploaded {file_path} to gs://{bucket_name}/{destination}')


def upload_hist_data(*op_args):
    bucket_name = op_args[0]
    destination_prefix = op_args[1]
    csv_filepath = op_args[2]
    upload_to_gcs(bucket_name, destination_prefix, csv_filepath)
    return destination_prefix
