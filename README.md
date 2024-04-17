# H2HDB

## Description

H2HDB is a comprehensive database for organising and managing H@H comic collections. It offers a streamlined way to catalogue your comics, providing key information such as GID (Gallery ID), title, tags and more, ensuring your collection is always organised and accessible.

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
    ./.venv/Scripts/python -m pip install -e ./h2hdb/[mysal,cbz,komga] # Install the h2hdb packages.
    rm -rf ./h2hdb/ # Remove the downloaded 'h2hdb' folder.
    ```

4. Run the script.

    ```bash
    ./.venv/Scripts/python -m h2hdb --config [json-path]
    ```

### Config

```json
{
    "h2h": {
        "download_path": "[str]", // The download path of H@H. The default is `download`.
        "cbz_path": "[str]", // The cbz in this path.
        "cbz_max_size": "[int]", // The maxinum of the mininum of width and height height. The default is `768`.
        "cbz_grouping": "[str]" // `flat`, `date-yyyy`, `date-yyyy-mm`, or `date-yyyy-mm-dd`. The default is `flat`.
    },
    "database": {
        "sql_type": "[str]", // Now only supports `mysql`. The default is `mysql`.
        "host": "[str]", // The default is `localhost`.
        "port": "[str]", // String, not Integer. The default is `3306`.
        "user": "[str]", // The default is `root`.
        "password": "[str]" // The default is `password`.
    },
    "multiprocess": {
        "number": "[int]" // If it is not 1, the logger will not be used.
    },
    "logger": {
        "level": "[str]", // One of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL.
        "display_on_screen": "[bool]", // The default is `false`.
        "max_log_entry_length": "[int]", // Maximum length of log entries.
        "write_to_file": "[str]" // Path to write log entries to.
    },
    "media_server": {
        "server_type": "[str]", // Now only supports `komga`. The defult is ``.
        "server_config": {
            "base_url": "[url]", // The media server's base url.
            "api_username": "[str]", // The media server's administrator.
            "api_password": "[str]", // The password of the media server's adimistrator.
            "library_id": "[str]" // The libary ID for komga.
        } // The defult is `{}`.
    }
}
```

## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

## License

This project is distributed under the terms of the GNU General Public Licence (GPL). For detailed licence terms, see the `LICENSE` file included in this distribution.
