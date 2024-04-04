__all__ = ["parse_gallery_info"]


import os
import datetime


class GalleryInfoParser:
    """
    A class that represents a parser for gallery information.

    Attributes:
        gallery_name (str): The name of the gallery.
        gid (int): The gallery ID.
        files_path (list[str]): The paths of the files in the gallery.
        modified_time (str): The modified time of the gallery.
        title (str): The title of the gallery.
        upload_time (str): The upload time of the gallery.
        galleries_comments (str): The uploader's comment for the gallery.
        upload_account (str): The account used to upload the gallery.
        download_time (str): The download time of the gallery.
        tags (dict[str, str]): The tags associated with the gallery.
    """

    def __init__(
        self,
        gallery_name: str,
        gid: int,
        files_path: list[str],
        modified_time: str,
        title: str,
        upload_time: str,
        galleries_comments: str,
        upload_account: str,
        download_time: str,
        tags: dict[str, str],
    ) -> None:
        self.gallery_name = gallery_name
        self.gid = gid
        self.files_path = files_path
        self.modified_time = modified_time
        self.title = title
        self.upload_time = upload_time
        self.galleries_comments = galleries_comments
        self.upload_account = upload_account
        self.download_time = download_time
        self.tags = tags

    __slots__ = [
        "gallery_name",
        "gid",
        "files_path",
        "modified_time",
        "title",
        "upload_time",
        "galleries_comments",
        "upload_account",
        "download_time",
        "tags",
    ]


def parse_gallery_info(folder_path: str) -> GalleryInfoParser:
    """
    Parses the gallery information from the given folder path.

    Args:
        folder_path (str): The path to the folder containing the gallery information.

    Returns:
        GalleryInfoParser: An instance of the GalleryInfoParser class containing the parsed gallery information.
    """
    gallery_info_path = os.path.join(folder_path, "galleryinfo.txt")
    with open(gallery_info_path, "r", encoding="utf-8") as file:
        lines = file.read().strip("\n").split("\n")

    gallery_name = os.path.basename(folder_path)
    if "[" in gallery_name and "]" in gallery_name:
        gid = int(gallery_name.split("[")[-1].replace("]", ""))
    else:
        gid = int(gallery_name)
    files_path = os.listdir(folder_path)
    modified_time = datetime.datetime.fromtimestamp(
        os.path.getmtime(gallery_info_path)
    ).strftime("%Y-%m-%d %H:%M:%S")

    comments = False
    comment_lines = list()
    for line in lines:
        if "Uploader's Comments" in line:
            comments = True
        elif comments:
            comment_lines.append(line.strip())
        elif ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            match key:
                case "Tags":
                    tags = dict[str, str]()
                    for tag in value.split(","):
                        if ":" in tag:
                            tag_key, tag_value = tag.split(":", 1)
                            tags[tag_key.strip()] = tag_value.strip()
                        else:
                            tags["no_tag"] = tag.strip()
                case "Title":
                    title = value
                case "Upload Time":
                    upload_time = value
                case "Uploaded By":
                    upload_account = value
                case "Downloaded":
                    download_time = value

    uploader_comment = "\n".join(comment_lines).strip("\n")

    return GalleryInfoParser(
        gallery_name,
        gid,
        files_path,
        modified_time,
        title,
        upload_time,
        uploader_comment,
        upload_account,
        download_time,
        tags,
    )
