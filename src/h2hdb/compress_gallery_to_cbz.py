__all__ = [
    "compress_images_and_create_cbz",
    "expected_output_filename",
    "gallery_name_to_cbz_file_name",
]

import shutil
import uuid
import zipfile
from pathlib import Path
from typing import cast

from PIL import Image, ImageFile

from .gallery_source_manifest import cbz_input_manifest_to_comment
from .settings import (
    COMPARISON_HASH_ALGORITHM,
    FILE_NAME_LENGTH_LIMIT,
    hash_function_by_file,
)

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".bmp")


def compress_image(image_path: Path, output_path: Path, max_size: int) -> None:
    """Compress an image, saving it to the output path."""
    with Image.open(image_path) as opened_image:
        image = cast(Image.Image, opened_image)
        if image.mode in ("RGBA", "LA"):
            image = image.convert("RGBA")
            white_bg = Image.new("RGBA", image.size, (255, 255, 255, 255))
            image = Image.alpha_composite(white_bg, image)
            image = image.convert("RGB")
        if image.mode != "RGB":
            image = image.convert("RGB")

        max_width = max_size
        max_height = max_size
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
        image.thumbnail((max_width, max_height), resample=Image.Resampling.LANCZOS)
        if image.format in unsuitable_formats:
            image.save(output_path, image.format)
        else:
            if "xmp" in image.info:
                del image.info["xmp"]
            image.save(output_path, "JPEG")


def create_cbz(
    directory: Path,
    output_path: Path,
    input_manifest: bytes,
) -> None:
    """Create a CBZ file from all images in a directory."""
    # Written to a sibling temp file and atomically moved into place so a
    # process killed mid-write (OOM, crash) never leaves a corrupt file at
    # output_path -- only a stray temp file, which
    # H2HDBCBZFiles._refresh_current_cbz_files() clears out on the next run
    # since it doesn't match any expected CBZ file name. The temp name is
    # independent of output_path's name (rather than output_path + a
    # suffix) since gallery names are already truncated to the filesystem's
    # name-length limit and couldn't take a suffix.
    tmp_output_path = output_path.parent / f".{uuid.uuid4().hex}.cbz.tmp"
    with zipfile.ZipFile(tmp_output_path, "w") as cbz:
        for entry in directory.iterdir():
            cbz.write(entry, entry.name)
        cbz.comment = cbz_input_manifest_to_comment(input_manifest)
    tmp_output_path.replace(output_path)


def expected_output_filename(filename: str) -> str:
    """Name a file would have inside the cbz, if its hash isn't excluded."""
    if filename.lower().endswith(IMAGE_EXTENSIONS):
        return f"{Path(filename).stem}.jpg"
    return filename


def hash_and_process_file(
    input_directory: Path,
    tmp_cbz_directory: Path,
    filename: str,
    exclude_hashs: set[bytes],
    max_size: int,
) -> None:
    file_hash = hash_function_by_file(
        input_directory / filename, COMPARISON_HASH_ALGORITHM
    )
    if file_hash not in exclude_hashs:
        if filename.lower().endswith(IMAGE_EXTENSIONS):
            new_filename = expected_output_filename(filename)
            compress_image(
                input_directory / filename,
                tmp_cbz_directory / new_filename,
                max_size,
            )
        elif filename.lower().endswith(".gif"):
            compress_image(
                input_directory / filename,
                tmp_cbz_directory / filename,
                max_size,
            )
        else:
            shutil.copy(
                input_directory / filename,
                tmp_cbz_directory / filename,
            )


def compress_images_and_create_cbz(
    input_directory: Path,
    output_directory: Path,
    tmp_directory: Path,
    max_size: int,
    exclude_hashs: set[bytes],
    input_manifest: bytes,
) -> None:
    if len(set([input_directory, output_directory, tmp_directory])) < 2:
        raise ValueError("Input and output directories cannot be the same.")

    gallery_name = input_directory.name
    tmp_cbz_directory = tmp_directory / gallery_name
    if tmp_cbz_directory.exists():
        shutil.rmtree(tmp_cbz_directory)
    tmp_cbz_directory.mkdir(parents=True)

    for entry in input_directory.iterdir():
        hash_and_process_file(
            input_directory, tmp_cbz_directory, entry.name, exclude_hashs, max_size
        )

    output_directory.mkdir(parents=True, exist_ok=True)
    cbzfile = output_directory / gallery_name_to_cbz_file_name(gallery_name)
    create_cbz(tmp_cbz_directory, cbzfile, input_manifest)
    shutil.rmtree(tmp_cbz_directory)


def gallery_name_to_cbz_file_name(gallery_name: str) -> str:
    """Convert a gallery name to a CBZ file name."""
    while (len(gallery_name.encode("utf-8")) + 4) > FILE_NAME_LENGTH_LIMIT:
        gallery_name = gallery_name[1:]
    return gallery_name + ".cbz"
