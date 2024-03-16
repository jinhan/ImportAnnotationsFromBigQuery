import pandas as pd
from geoalchemy2 import WKBElement
from shapely.geometry import Point
import gcsfs
import parameters as param
from data import write_files_to_bucket
import datetime

def create_wkt_element(geom):
    return WKBElement(geom.wkb)


def format_points(data):
    li = data.strip('[]').split()
    data_final = [int(item) for item in li]
    return data_final


def process_chunk(chunk, last_index):
    chunk['id'] = chunk.index + last_index + 1
    chunk['id'] = chunk['id'].astype(int)
    chunk['created'] = datetime.datetime.utcnow()
    chunk['deleted'] = ''
    chunk["superceded_id"] = ''
    chunk['valid'] = 1
    chunk['pre_pt_position'] = chunk[['pre_pt_x', 'pre_pt_y', 'pre_pt_z']].values.tolist()
    chunk = chunk.drop(['pre_pt_x', 'pre_pt_y', 'pre_pt_z'], axis=1)
    chunk['post_pt_position'] = chunk[['post_pt_x', 'post_pt_y', 'post_pt_z']].values.tolist()
    chunk = chunk.drop(['post_pt_x', 'post_pt_y', 'post_pt_z'], axis=1)
    chunk['ctr_pt_position'] = chunk[['x', 'y', 'z']].values.tolist()
    chunk = chunk.drop(['x', 'y', 'z'], axis=1)
    chunk['ctr_pt_position'] = chunk.ctr_pt_position.apply(Point)
    chunk['post_pt_position'] = chunk.post_pt_position.apply(Point)
    chunk['pre_pt_position'] = chunk.pre_pt_position.apply(Point)

    chunk['pre_pt_position'] = chunk['pre_pt_position'].apply(create_wkt_element)
    chunk['ctr_pt_position'] = chunk['ctr_pt_position'].apply(create_wkt_element)
    chunk['post_pt_position'] = chunk['post_pt_position'].apply(create_wkt_element)
    chunk['size'] = None
    chunk = chunk.reindex(
        ['id','created','deleted','superceded_id','valid','pre_pt_position','post_pt_position','ctr_pt_position', 'size'], axis=1)
    return chunk

def reformat_write(file_path, last_index):
    fs = gcsfs.GCSFileSystem(project=param.export_project_id, token=param.export_key_path)

    with fs.open(f'{param.export_bucket_name}/{file_path}', 'rb') as file:
        chunks = pd.read_csv(file, chunksize=10000)

        formatted_chunk_list = []
        for i, chunk in enumerate(chunks):
            # print(f"Chunk {str(i)} / len({len(file_path)})")
            formatted_chunk = process_chunk(chunk, last_index)
            formatted_chunk_list.append(formatted_chunk)
        
        df_concat = pd.concat(formatted_chunk_list)
        write_files_to_bucket(df_concat)

        last_index = last_index + df_concat.shape[0]

    return last_index

# if __name__ == "__main__":
#     chunks = pd.read_csv(csv_file, chunksize=10000)
#
#     created_time_stamp = datetime.datetime.utcnow()
#
#     for i, chunk in enumerate(chunks):
#         print(f"Chunk {str(i)} / len(f"
#         len(csv_file)}")
#         formatted_chunk = process_chunk(chunk, created_time_stamp)
#         formatted_chunk.to_csv(f'formatted_synapse_annotationss_{created_time_stamp}.csv',
#                                header=False, mode='a', index=False)