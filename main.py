from data import get_bucket_file_names
from reformat import reformat_write

def start():
    file_paths = get_bucket_file_names()

    last_index = 0
    for i, file_path in enumerate(file_paths):
        print(f"# {str(i)} / len({len(file_paths)}): {file_path}, {last_index}")
        if i > 1:
            last_index = reformat_write(file_path, last_index)
    return ''

if __name__ == "__main__":
    start()