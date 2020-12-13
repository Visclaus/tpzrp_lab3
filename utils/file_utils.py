import os


def create_if_not_exist(file_path):
    if not os.path.exists(file_path):
        open(file_path, "w+b").close()
