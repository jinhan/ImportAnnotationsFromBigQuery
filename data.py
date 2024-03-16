from google.cloud import storage
import gcsfs
import parameters as param
import datetime
import pandas as pd

def get_bucket_file_names():
    client = storage.Client.from_service_account_json(param.export_key_path)
    bucket = storage.Bucket(client, param.export_bucket_name)
    blobs = client.list_blobs(bucket, prefix=param.export_folder_path)
    csv_files = [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]

    return csv_files

def write_files_to_bucket(df):
    fs = gcsfs.GCSFileSystem(project=param.import_project_id, token=param.import_key_path)

    csv_data = df.to_csv(header=False, mode='a', index=False)

    created_time_stamp = datetime.datetime.utcnow()

    path = f'formatted_synapse_annotations_{created_time_stamp}.csv'

    with fs.open(f'{param.import_bucket_name}/{param.import_folder_path}/{path}', 'wb') as file:
        file.write(csv_data.encode())


# if __name__ == '__main__':
#     # file_names = get_bucket_file_names(bucket_name, folder_path, gcs_key_path)
#     # print(file_names)
#     import pandas as pd
#     from reformat import process_chunk
#     chunks = pd.read_csv('formatted_synapse_annotations_2023-06-29 17:34:03.606402.csv', chunksize=10000)
#     for i, chunk in enumerate(chunks):
#         if i==0:
#             print(f"Chunk {str(i)}")
#             formatted_chunk = process_chunk(chunk)
#             write_files_to_bucket(formatted_chunk)


