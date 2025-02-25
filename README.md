# H2HDB

## Description

The `H2HDB` is a comprehensive database for organising and managing H@H comic collections. It offers a streamlined way to catalogue your comics, providing key information such as GID (Gallery ID), title, tags and more, ensuring your collection is always organised and accessible.

---

## Features

- [x] Add new galleries to the database
- [x] Comporess H@H's galleries to a folder
- [x] Record the removed GIDs in a separate list
- [ ] Write document (need?)

---

## Installation and Usage

1. Install Python 3.13 or higher from [python.org](https://www.python.org/downloads/).
1. Install the required packages.

    ```bash
    pip install h2hdb
    ```

1. Run the script.

    ```bash
    python -m h2hdb --config [json-path]
    ```

### Config

```json
{
    "h2h": {
        "download_path": "[str]", // The download path of H@H. The default is `download`.
        "cbz_path": "[str]", // The cbz in this path.
        "cbz_max_size": "[int]", // The maxinum of the mininum of width and height height. The default is `768`.
        "cbz_grouping": "[str]", // `flat`, `date-yyyy`, `date-yyyy-mm`, or `date-yyyy-mm-dd`. The default is `flat`.
        "cbz_sort": "[str]" // `upload_time`, `download_time`, `pages`, or `pages+[num]`. The default is `no`.
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
    }
}
```

---

## Q & A

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
