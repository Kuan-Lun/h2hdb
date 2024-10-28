# H2HDB

## Description

The `H2HDB` is a comprehensive database for organising and managing H@H comic collections. It offers a streamlined way to catalogue your comics, providing key information such as GID (Gallery ID), title, tags and more, ensuring your collection is always organised and accessible.

---

## Features

- [x] Add new galleries to the database
- [x] Comporess H@H's galleries to a folder
- [x] Add the galleries' tags to Komga
- [x] Record the removed GIDs in a separate list
- [ ] Write document (need?)

---

## Installation and Usage

1. Install Python 3.12 or higher from [python.org](https://www.python.org/downloads/).
1. Clone the repository.

    ```bash
    git clone https://github.com/Kuan-Lun/h2hdb.git # It will download a folder 'h2hdb'.
    ```

1. Install the required packages.

    ```bash
    python -m venv .venv # Create a virtual environment.
    ./.venv/Scripts/python -m pip install -r ./h2hdb/requirements.txt # Install the required packages.
    ./.venv/Scripts/python -m pip install -e ./h2hdb/[mysal,cbz,komga,synochat] # Install the h2hdb packages.
    rm -rf ./h2hdb/ # Remove the downloaded 'h2hdb' folder.
    ```

1. Run the script.

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
        "cbz_grouping": "[str]", // `flat`, `date-yyyy`, `date-yyyy-mm`, or `date-yyyy-mm-dd`. The default is `flat`.
        "cbz_sort": "[str]" // `upload_time`, `download_time`, `pages`, or `pages+[num]`. The default is `upload_time`.
    },
    "database": {
        "sql_type": "[str]", // Now only supports `mysql`. The default is `mysql`.
        "host": "[str]", // The default is `localhost`.
        "port": "[str]", // String, not Integer. The default is `3306`.
        "user": "[str]", // The default is `root`.
        "password": "[str]" // The default is `password`.
    },
    "logger": {
        "level": "[str]" // One of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL.
    },
    "media_server": {
        "server_type": "[str]", // Now only supports `komga`. The defult is ``.
        "server_config": {
            "base_url": "[url]", // The media server's base url.
            "api_username": "[str]", // The media server's administrator.
            "api_password": "[str]", // The password of the media server's adimistrator.
            "library_id": "[str]" // The libary ID for komga.
        } // The defult is null.
    }
}
```

---

## Q & A

- How to use Komga?
See [Rainie's article](https://home.gamer.com.tw/artwork.php?sn=5659465).

- How to find library IDs in Komga?
To find your library ID, log into komga's library. Look at the URL in your browser’s address bar, which will be formatted like this: `[base_url]/libraries/[library_id]/series`. In this URL, the `[library_id]` part is your library ID.

- Why aren't the tags for CBZ-files in Komga updated?
When you first run `H2HDB`, it generates CBZ-files. These CBZ-files are not immediately visible in Komga's library. To update them, you have two options: you can either click the 'scan library files' button in Komga, or you can run `H2HDB` twice. The first run scans the library, and the second run updates the tags.

- Why are some images missing from the CBZ-files?
`H2HDB` does not compress images that are considered spam according to certain rules. If you encounter any images that you believe should have been included, please report the issue.

- Why are some images in some CBZ files and not in other CBZ-files?
`H2HDB` learns the spam rule from the previous CBZ files. If you kill the CBZ files containing these images, the new CBZ files will not contain these images.

---

## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

---

## License

This project is distributed under the terms of the GNU General Public Licence (GPL). For detailed licence terms, see the `LICENSE` file included in this distribution.
