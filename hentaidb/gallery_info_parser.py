__all__ = ["get_gallery_info_path", "parse_gallery_info"]


import os
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
        gallery_name: str,
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
        self.gallery_name = gallery_name
        self.gid = gid
        self.files_path = files_path
        self.modified_time = modified_time
        self.title = title
        self.upload_time = upload_time
        self.uploader_comment = uploader_comment
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
        "uploader_comment",
        "upload_account",
        "download_time",
        "tags",
    ]


gallery_info_may_tag_type = str | dict[str, str]
gallery_info_content_type = int | str | dict[str, str] | list[str]
gallery_info_type = dict[str, gallery_info_content_type]


def parse_gallery_info(
    folder_path: str,
) -> GalleryInfoParser:
    gallery_info_path = get_gallery_info_path(folder_path)
    with open(gallery_info_path, "r", encoding="utf-8") as file:
        lines = file.read().strip("\n").split("\n")

    gallery_name = os.path.basename(folder_path)
    gid = convert_gallery_name_to_gid(gallery_name)
    files_path = os.listdir(folder_path)
    modified_time = get_last_modified_time(gallery_info_path)

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


def convert_gallery_name_to_gid(gallery_name: str) -> int:
    if "[" in gallery_name and "]" in gallery_name:
        gid = int(gallery_name.split("[")[-1].replace("]", ""))
    else:
        gid = int(gallery_name)
    return gid
