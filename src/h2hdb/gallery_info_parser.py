__all__ = ["parse_gallery_info", "GalleryInfoParser"]


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

    __slots__ = [
        "gallery_folder",
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

    def __init__(
        self,
        gallery_folder: str,
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
        self.gallery_folder = gallery_folder
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

    def __repr__(self) -> str:
        return f"GalleryInfoParser(gallery_name={self.gallery_name}, gid={self.gid}, files_path={self.files_path}, modified_time={self.modified_time}, title={self.title}, upload_time={self.upload_time}, galleries_comments={self.galleries_comments}, upload_account={self.upload_account}, download_time={self.download_time}, tags={self.tags})"

    def __str__(self) -> str:
        return self.__repr__()


def parse_gallery_info(gallery_folder: str) -> GalleryInfoParser:
    """
    Parses the gallery information from the given folder path.

    Args:
        gallery_folder (str): The path to the folder containing the gallery information.

    Returns:
        GalleryInfoParser: An instance of the GalleryInfoParser class containing the parsed gallery information.
    """
    gallery_info_path = os.path.join(gallery_folder, "galleryinfo.txt")
    with open(gallery_info_path, "r", encoding="utf-8") as file:
        lines = file.read().strip("\n").split("\n")

    gallery_name = os.path.basename(gallery_folder)
    if "[" in gallery_name and "]" in gallery_name:
        gid = int(gallery_name.split("[")[-1].replace("]", ""))
    else:
        gid = int(gallery_name)
    files_path = os.listdir(gallery_folder)
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
                            if tag_key.strip() != "":
                                tags[tag_key.strip()] = tag_value.strip()
                            else:
                                tags["untagged"] = tag_value.strip()
                        else:
                            tags["untagged"] = tag.strip()
                case "Title":
                    title = value
                case "Upload Time":
                    upload_time = value
                case "Uploaded By":
                    upload_account = value
                case "Downloaded":
                    download_time = value

    galleries_comments = "\n".join(comment_lines).strip("\n")

    return GalleryInfoParser(
        gallery_folder=gallery_folder,
        gallery_name=gallery_name,
        gid=gid,
        files_path=files_path,
        modified_time=modified_time,
        title=title,
        upload_time=upload_time,
        galleries_comments=galleries_comments,
        upload_account=upload_account,
        download_time=download_time,
        tags=tags,
    )
