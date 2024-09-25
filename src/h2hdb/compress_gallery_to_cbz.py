__all__ = ["compress_images_and_create_cbz", "calculate_hash_of_file_in_cbz"]

import os
from PIL import Image, ImageFile  # type: ignore
import zipfile
import shutil
import hashlib

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True

from .settings import FILE_NAME_LENGTH_LIMIT, COMPARISON_HASH_ALGORITHM
from .settings import hash_function_by_file


def compress_image(image_path: str, output_path: str, max_size: int) -> None:
    """Compress an image, saving it to the output path."""
    with Image.open(image_path) as image:
        if image.mode in ("RGBA", "LA"):
            image = image.convert("RGBA")
            white_bg = Image.new("RGBA", image.size, (255, 255, 255, 255))
            image = Image.alpha_composite(white_bg, image)
            image = image.convert("RGB")
        if image.mode != "RGB":
            image = image.convert("RGB")

        if max_size >= 1:
            if image.height >= image.width:
                max_width = max_size
                scale = max_size / image.width
                max_height = int(image.height * scale)
            else:
                max_height = max_size
                scale = max_size / image.height
                max_width = int(image.width * scale)

        unsuitable_formats = ["GIF", "TIFF", "ICO"]
        image.thumbnail((max_width, max_height), resample=Image.LANCZOS)
        if image.format in unsuitable_formats:
            image.save(output_path, image.format)
        else:
            image.save(output_path, "JPEG")


def create_cbz(directory, output_path) -> None:
    """Create a CBZ file from all images in a directory."""
    with zipfile.ZipFile(output_path, "w") as cbz:
        for filename in os.listdir(directory):
            cbz.write(os.path.join(directory, filename), filename)


def hash_and_process_file(
    input_directory: str,
    tmp_cbz_directory: str,
    filename: str,
    exclude_hashs: list[bytes],
    max_size: int,
) -> None:
    file_hash = hash_function_by_file(
        os.path.join(input_directory, filename), COMPARISON_HASH_ALGORITHM
    )
    base, ext = os.path.splitext(filename)
    if file_hash not in exclude_hashs:
        if filename.lower().endswith((".jpg", ".jpeg", ".png", "bmp")):
            new_filename = base + ".jpg"
            compress_image(
                os.path.join(input_directory, filename),
                os.path.join(tmp_cbz_directory, new_filename),
                max_size,
            )
        elif filename.lower().endswith(".gif"):
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


# Compress images and create a CBZ file
def compress_images_and_create_cbz(
    input_directory: str,
    output_directory: str,
    tmp_directory: str,
    max_size: int,
    exclude_hashs: list[bytes],
) -> None:
    if len(set([input_directory, output_directory, tmp_directory])) < 2:
        raise ValueError("Input and output directories cannot be the same.")

    # Create the output directory
    gallery_name = os.path.basename(input_directory)
    tmp_cbz_directory = os.path.join(tmp_directory, gallery_name)
    if os.path.exists(tmp_cbz_directory):
        shutil.rmtree(tmp_cbz_directory)
    os.makedirs(tmp_cbz_directory)

    for filename in os.listdir(input_directory):
        hash_and_process_file(
            input_directory, tmp_cbz_directory, filename, exclude_hashs, max_size
        )

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
    if zipfile.is_zipfile(cbz_path):
        with zipfile.ZipFile(cbz_path, "r") as myzip:
            with myzip.open(file_name) as myfile:
                file_content = myfile.read()
                hash_object = hashlib.new(algorithm)
                hash_object.update(file_content)
                hash_of_file = hash_object.digest()
    else:
        hash_of_file = bytes(0)
    return hash_of_file
