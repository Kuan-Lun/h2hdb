# H2HDB
## Description

H2HDB is a comprehensive database for organising and managing H@H (Hitomi la@Home) comic collections. It offers a streamlined way to catalogue your comics, providing key information such as GID (Gallery ID), title, tags and more, ensuring your collection is always organised and accessible.

## Features

- [x] Add new galleries to the database
- [ ] Edit existing gallery details
- [ ] View a list of all galleries in the collection
    - [x] `select_[tablename]` functions
    - [ ] Write document
- [ ] Delete galleries from the database
- [x] Record the removed GIDs in a separate list

## Installation and Usage

1. Install Python 3.12 or higher from [python.org](https://www.python.org/downloads/).
2. Clone the repository.
    ```bash
    git clone [uri] # It will download a folder 'h2hdb'.
    ```
3. Install the required packages.
    ```bash
    python -m venv .venv # Create a virtual environment.
    ./.venv/Scripts/python -m pip install -r ./h2hdb/requirements.txt # Install the required packages.
    ./.venv/Scripts/python -m pip install -e ./h2hdb/ # Install the h2hdb package.
    rm -rf ./h2hdb/ # Remove the downloaded 'h2hdb' folder.
    ```
4. Run the script by running `./.venv/Scripts/python -m h2hdb --config [json-path]`.

### Config

```json
{
    "h2h": {
        "download_path": "[str]", // The download path of H@H. The default is `download`.
    },
    "database": {
        "sql_type": "[str]", // Now only supports `mysql`. The default is `mysql`.
        "host": "[str]", // The default is `localhost`.
        "port": "[str]", // String, not Integer. The default is `3306`.
        "user": "[str]", // The default is `root`.
        "password": "[str]" // The default is `password`.
    },
    "logger": {
        "level": "[str]", // One of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL.
        "display_on_screen": "[bool]", // The default is `false`.
        "max_log_entry_length": "[int]", // Maximum length of log entries.
        "write_to_file": "[str]" // Path to write log entries to.
    }
}
```


### Tables

There are two SQL views in the database:

1. `galleries_infos`: This table contains information about the galleries in the database. It contains columns such as `db_gallery_id`, `name`, `title`, `gid`, `upload_account`, `upload_time`, `download_time`, `modified_time`, and `access_time`. Here is an example of the table:

    | db_gallery_id | name                  | title           | gid | upload_account | upload_time         | download_time       | modified_time       | access_time         |
    | ------------- | --------------------- | --------------- | --- | -------------- | ------------------- | ------------------- | ------------------- | ------------------- |
    | 1             | [xxx] xxx [xxx] [123] | [xxx] xxx [xxx] | 123 | alice          | 2019-08-03 06:16:00 | 2023-02-09 11:26:00 | 2023-02-09 19:26:05 | 2023-02-09 11:26:00 |
    | 2             | 456                   | A long name     | 456 | bob            | 2019-08-30 16:01:00 | 2020-10-15 17:15:00 | 2020-10-16 01:16:47 | 2020-10-15 17:15:00 |

2. `files_hashs`: This table contains information about the files in the galleries. It contains columns like `db_file_id`, `gallery_title`, `gallery_name`, `file_name`, `sha224` and more. Here is an example of the table:

    | db_file_id | gallery_title        | gallery_name    | file_name | sha224 | ... |
    | ---------- | -------------------- | --------------- | --------- | ------ | --- |
    | 1          | [xxx] xxx [xxx] [123] | [xxx] xxx [xxx] | 001.jpg   | 1ab... | ... |
    | 37         | A long name          | 456             | 1.png     | a8f... | ... |

#### Gallery Information

| Name                   | Description                                                                                   |
| :---------------------- | :----------------------------------------------------------------------------------------- |
| `galleries_names`      | The folder name of the gallery downloaded from H@H.                                      |
| `galleries_gids`       | The GID of the gallery. This value is parsed from the folder of the downloaded gallery. |
| `galleries_download_times` `galleries_upload_accounts` `galleries_upload_times` `galleries_titles` |  They are extracted from `galleryinfo.txt`. |
| `galleries_tags_[category]` | The value of the `[category]` tag in `galleryinfo.txt`. For example, `galleries_tags_artist` can be the name of a table that collects `artist:alice` as `alice`.<ul><li>The `galleries_tags_` collects the tag value from `galleryinfo.txt` without specifying the category name. For example `group`.</li><li>The `galleries_tags_no_tag` gets the tag value if the value of the `[category]` tag in `galleryinfo.txt` is empty. For example `:group`.</li><ul> |
| `removed_galleries_gids` | Record the GID of the removed gallery. |
| `pending_gallery_removals` | All gallery names in this table will be deleted from all tables in the database after `python -m h2hdb --config [json-path]`. When the deletion is complete, the value in this table will be removed. |
| `pending_gallery_removals_no_tag` | The value of the tag `[category]` in `galleryinfo.txt` is empty. For example, `:group`. |
| `pending_gallery_removals_tags_[category]` | The value of the tag `[category]` in `galleryinfo.txt`. For example, `artist:alice`. |

#### Files in the Gallery's Folder

| Name                   | Description                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| `files_names` | The file name in the folder name of the download gallery is H@H. |
| `files_hashs_[algorithm]` | The values of the hash algorithm `[algorithm]` are `blake2b`, `blake2s`, `sha1`, `sha224`, `sha256`, `sha384`, `sha3_224`, `sha3_256`, `sha3_384`, `sha3_512`, `sha512`. |


### Examples

Here is an example of how to use the `h2hdb` package to add gallery information and removed gallery GIDs to the database.
```python
    import os

    from h2hdb import H2HDB, load_config
    
    config = load_config(os.path.join("Your config path"))
    with H2HDB(config) as connector:
        connector.insert_gallery_info("Gallery folder path") # Insert gallery information to database
        connector.insert_removed_gallery_gid(123) # Insert removed gallery GID
```
## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

## License

This project is distributed under the terms of the GNU General Public Licence (GPL). For detailed licence terms, see the `LICENSE` file included in this distribution.