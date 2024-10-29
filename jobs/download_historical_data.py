import requests
import os


def fetch_metadata(collectionId):
    url = f'https://api-production.data.gov.sg/v2/public/api/collections/{collectionId}/metadata?withDatasetMetadata=true'
    response = requests.get(url)
    metadata = response.json()
    
    dataset_ids = [i['datasetId'] for i in metadata['data']['datasetMetadata']]
    return dataset_ids

def download_csv_files(dataset_ids, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    csv_files = []
    
    for datasetId in dataset_ids:
        download_url = f'https://api-open.data.gov.sg/v1/public/api/datasets/{datasetId}/poll-download'
        response = requests.get(download_url, headers={'Content-Type': 'application/json'}, json={})
        result = response.json()
        download_url = result['data']['url']
        
        file_response = requests.get(download_url)
        filename = result['data']['url'].split('filename%3D')[1].split('%22')[1]
        output_file = os.path.join(output_dir, filename)

        with open(output_file, 'wb') as f:
            f.write(file_response.content)
        
        csv_files.append(output_file)
        print(f'File {filename} downloaded successfully')
    
    return csv_files

def download_data(*op_args):
    collectionId = op_args[0]
    dataset_ids = fetch_metadata(collectionId)
    output_dir = f'/opt/airflow/result/api_download/'
    csv_files = download_csv_files(dataset_ids, output_dir)
    return csv_files