from typing import Union
import os


def parse_gallery_info(
    file_path: str,
) -> Union[dict[str, str], dict[str, dict[str, str]]]:
    with open(
        os.path.join(file_path, "galleryinfo.txt"), "r", encoding="utf-8"
    ) as file:
        lines = file.readlines()

    info = {}
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
    return info
