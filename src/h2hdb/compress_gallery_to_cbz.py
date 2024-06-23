__all__ = ["compress_images_and_create_cbz", "calculate_hash_of_file_in_cbz"]

import os
from PIL import Image, ImageFile  # type: ignore
import zipfile
import shutil
import hashlib

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True

from .settings import FILE_NAME_LENGTH_LIMIT, COMPARISON_HASH_ALGORITHM
from .settings import hash_function
from .threading_tools import CBZThreadsList


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
        resample = Image.LANCZOS if image.format == "JPEG" else Image.BICUBIC  # type: ignore
        image.thumbnail((max_width, max_height), resample=resample)
        image.save(output_path, image.format)


def create_cbz(directory, output_path):
    """Create a CBZ file from all images in a directory."""
    with zipfile.ZipFile(output_path, "w") as cbz:
        for filename in os.listdir(directory):
            cbz.write(os.path.join(directory, filename), filename)


# Compress images and create a CBZ file
def compress_images_and_create_cbz(
    input_directory: str,
    output_directory: str,
    tmp_directory: str,
    max_size: int,
    exclude_hashs: list,
) -> None:
    if len(set([input_directory, output_directory, tmp_directory])) < 2:
        raise ValueError("Input and output directories cannot be the same.")

    # Create the output directory
    gallery_name = os.path.basename(input_directory)
    tmp_cbz_directory = os.path.join(tmp_directory, gallery_name)
    if os.path.exists(tmp_cbz_directory):
        shutil.rmtree(tmp_cbz_directory)
    os.makedirs(tmp_cbz_directory)

    def hash_and_process_file(filename: str) -> None:
        with open(os.path.join(input_directory, filename), "rb") as file:
            file_content = file.read()
        file_hash = hash_function(file_content, COMPARISON_HASH_ALGORITHM)

        if file_hash not in exclude_hashs:
            if filename.lower().endswith((".jpg", ".jpeg", ".png", ".gif")):
                compress_image(
                    os.path.join(input_directory, filename),
                    os.path.join(tmp_cbz_directory, filename),
                    max_size,
                )
            else:
                shutil.copy(
                    os.path.join(input_directory, filename),
                    os.path.join(tmp_cbz_directory, filename),
                )

    with CBZThreadsList() as threads:
        for filename in os.listdir(input_directory):
            threads.append(target=hash_and_process_file, args=(filename,))

    # Create the CBZ file
    os.makedirs(output_directory, exist_ok=True)
    cbzfile = os.path.join(
        output_directory, gallery_name_to_cbz_file_name(gallery_name)
    )
    create_cbz(tmp_cbz_directory, cbzfile)
    shutil.rmtree(tmp_cbz_directory)


def gallery_name_to_cbz_file_name(gallery_name: str) -> str:
    """Convert a gallery name to a CBZ file name."""
    while (len(gallery_name.encode("utf-8")) + 4) > FILE_NAME_LENGTH_LIMIT:
        gallery_name = gallery_name[1:]
    return gallery_name + ".cbz"


def calculate_hash_of_file_in_cbz(
    cbz_path: str, file_name: str, algorithm: str
) -> bytes:
    with zipfile.ZipFile(cbz_path, "r") as myzip:
        with myzip.open(file_name) as myfile:
            file_content = myfile.read()
            hash_object = hashlib.new(algorithm)
            hash_object.update(file_content)
            return hash_object.digest()
