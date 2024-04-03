__all__ = ["get_gallery_info_path", "parse_gallery_info"]


from typing import Union
import os
from copy import deepcopy
import datetime


def get_last_modified_time(file: str):
    return datetime.datetime.fromtimestamp(os.path.getmtime(file)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def get_gallery_info_path(folder_path: str) -> str:
    return os.path.join(folder_path, "galleryinfo.txt")


class GalleryInfoParser:
    def __init__(
        self,
        db_gallery_id: str,
        gid: int,
        files_path: list[str],
        modified_time: str,
        title: str,
        upload_time: str,
        uploader_comment: str,
        upload_account: str,
        download_time: str,
        tags: dict[str, str],
    ) -> None:
        self.db_gallery_id = db_gallery_id
        self.gid = gid
        self.files_path = files_path
        self.modified_time = modified_time
        self.title = title
        self.upload_time = upload_time
        self.uploader_comment = uploader_comment
        self.upload_account = upload_account
        self.download_time = download_time
        self.tags = tags


def parse_gallery_info(
    folder_path: str,
) -> GalleryInfoParser:
    gallery_info_path = get_gallery_info_path(folder_path)
    with open(gallery_info_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    info = dict()
    info["db_gallery_id"] = os.path.basename(folder_path)
    info["gid"] = convert_gallery_dbid_to_gid(info["db_gallery_id"])
    info["files_path"] = os.listdir(folder_path)
    info["modified_time"] = get_last_modified_time(gallery_info_path)
    comments = False
    comment_lines = []
    for line in lines:
        if "Uploader's Comments" in line:
            comments = True
            continue
        if comments:
            comment_lines.append(line.strip())
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()

            if key == "Tags":
                tags = {}
                for tag in value.split(","):
                    if ":" in tag:
                        tag_key, tag_value = tag.split(":", 1)
                        tags[tag_key.strip()] = tag_value.strip()
                    else:
                        tags["no_tag"] = tag.strip()
                value = tags

            info[key] = value

    info["Uploader's Comments"] = "\n".join(comment_lines)

    info = convert_keys_to_comicdb(info)
    return GalleryInfoParser(**info)


def convert_keys_to_comicdb(
    info: Union[dict[str, str], dict[str, dict[str, str]]],
) -> Union[dict[str, str], dict[str, dict[str, str]]]:
    info["title"] = info.pop("Title")
    info["upload_time"] = info.pop("Upload Time")
    info["uploader_comment"] = info.pop("Uploader's Comments")
    info["upload_account"] = info.pop("Uploaded By")
    info["download_time"] = info.pop("Downloaded")
    info["tags"] = info.pop("Tags")
    for key in deepcopy(info["tags"]):
        info["tags"][key] = info["tags"].pop(key)
    return info


def convert_gallery_dbid_to_gid(DB_Gallery_ID: str) -> int:
    if "[" in DB_Gallery_ID and "]" in DB_Gallery_ID:
        gid = int(DB_Gallery_ID.split("[")[-1].replace("]", ""))
    else:
        gid = int(DB_Gallery_ID)
    return gid
