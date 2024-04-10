__all__ = ["compress_images_and_create_cbz", "calculate_hash_of_file_in_cbz"]

import os
from PIL import Image # type: ignore
import zipfile
import shutil
import hashlib


def compress_image(image_path: str, output_path: str, max_size: int) -> None:
    """Compress an image, saving it to the output path."""
    with Image.open(image_path) as image:
        if max_size >= 1:
            if image.height > image.width:
                max_width = max_size
                scale = max_size / image.width
                max_height = int(image.height * scale)
            elif image.width > image.height:
                max_height = max_size
                scale = max_size / image.height
                max_width = int(image.width * scale)
            else:
                max_width = image.width
                max_height = image.height
        image.thumbnail((max_width, max_height))
        image.save(output_path, image.format)


def create_cbz(directory, output_path):
    """Create a CBZ file from all images in a directory."""
    with zipfile.ZipFile(output_path, "w") as cbz:
        for filename in os.listdir(directory):
            cbz.write(os.path.join(directory, filename), filename)

# Compress images and create a CBZ file
def compress_images_and_create_cbz(input_directory:str, output_directory: str, tmp_directory: str, max_size: int) -> None:
    if len(set([input_directory, output_directory, tmp_directory])) < 2:
        raise ValueError("Input and output directories cannot be the same.")

    # Create the output directory
    gallery_name = os.path.basename(input_directory)
    tmp_cbz_directory = os.path.join(tmp_directory, gallery_name)
    if os.path.exists(tmp_cbz_directory):
        shutil.rmtree(tmp_cbz_directory)
    os.makedirs(tmp_cbz_directory)

    # Compress the images
    for filename in os.listdir(input_directory):
        if filename.lower().endswith((".jpg", ".jpeg", ".png", ".gif")):
            compress_image(
                os.path.join(input_directory, filename),
                os.path.join(tmp_cbz_directory, filename),
                max_size
            )
        else:
            shutil.copy(os.path.join(input_directory, filename), os.path.join(tmp_cbz_directory, filename))

    # Create the CBZ file
    os.makedirs(output_directory, exist_ok=True)
    cbzfile = os.path.join(output_directory, gallery_name+".cbz")
    create_cbz(tmp_cbz_directory, cbzfile)
    shutil.rmtree(tmp_cbz_directory)

def calculate_hash_of_file_in_cbz(cbz_path: str, file_name: str, algorithm: str) -> bytes:
    with zipfile.ZipFile(cbz_path, 'r') as myzip:
        with myzip.open(file_name) as myfile:
            file_content = myfile.read()
            hash_object = hashlib.new(algorithm)
            hash_object.update(file_content)
            return hash_object.digest()

# 使用方式
# cbz_path = 'path_to_your_cbz_file'
# file_name = 'name_of_the_file_in_cbz'
# hash_value = calculate_hash_of_file_in_cbz(cbz_path, file_name)
# print(hash_value)