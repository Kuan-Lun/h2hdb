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


def parse_gallery_info(
    folder_path: str,
) -> Union[dict[str, str], dict[str, dict[str, str]]]:
    gallery_info_path = get_gallery_info_path(folder_path)
    with open(gallery_info_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    info = dict()
    info["DB_Gallery_ID"] = os.path.basename(folder_path)
    info["GID"] = convert_gallery_dbid_to_gid(info["DB_Gallery_ID"])
    info["Files_Path"] = os.listdir(folder_path)
    info["Modified_Time"] = get_last_modified_time(gallery_info_path)
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
                    tag_key, tag_value = tag.split(":", 1)
                    tags[tag_key.strip()] = tag_value.strip()
                value = tags

            info[key] = value

    info["Uploader's Comments"] = "\n".join(comment_lines)

    info = convert_keys_to_comicdb(info)
    return info


def convert_keys_to_comicdb(
    info: Union[dict[str, str], dict[str, dict[str, str]]],
) -> Union[dict[str, str], dict[str, dict[str, str]]]:
    info["Upload_Time"] = info.pop("Upload Time")
    info["Uploader_Comment"] = info.pop("Uploader's Comments")
    info["Upload_Account"] = info.pop("Uploaded By")
    info["Download_Time"] = info.pop("Downloaded")
    info["Tag"] = info.pop("Tags")
    for key in deepcopy(info["Tag"]):
        info["Tag"][key] = info["Tag"].pop(key)
    return info


def convert_gallery_dbid_to_gid(DB_Gallery_ID: str) -> int:
    if "[" in DB_Gallery_ID and "]" in DB_Gallery_ID:
        gid = int(DB_Gallery_ID.split("[")[-1].replace("]", ""))
    else:
        gid = int(DB_Gallery_ID)
    return gid
