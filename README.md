# H2HDB

## Description

The `H2HDB` is a comprehensive database for organising and managing H@H comic
collections. It offers a streamlined way to catalogue your comics, providing
key information such as GID (Gallery ID), title, tags and more, ensuring your
collection is always organised and accessible.

---

## Features

- [x] Add new galleries to the database
- [x] Comporess H@H's galleries to a folder
- [x] Record the removed GIDs in a separate list
- [ ] Write document (need?)

---

## Installation and Usage

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
   It manages the Python version and dependencies for you.
1. Install the required packages.

    ```bash
    uv pip install h2hdb
    ```

1. Run the script.

    ```bash
    uv run python -m h2hdb --config [json-path]
    ```

### Config

```json
{
    "h2h": {
        "download_path": "download",
        "cbz_path": "",
        "cbz_max_size": 768,
        "cbz_grouping": "flat",
        "cbz_sort": "no"
    },
    "database": {
        "sql_type": "mariadb",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "password",
        "database": "h2h"
    },
    "logger": {
        "level": "INFO"
    }
}
```

- `h2h.download_path`: H@H download path. The default is `download`.
- `h2h.cbz_path`: directory for CBZ output. The default is empty.
- `h2h.cbz_max_size`: maximum image size. The default is `768`.
- `h2h.cbz_grouping`: `flat`, `date-yyyy`, `date-yyyy-mm`, or
  `date-yyyy-mm-dd`. The default is `flat`.
- `h2h.cbz_sort`: `no`, `upload_time`, `download_time`, `gid`, `title`,
  `pages`, or `pages+[num]`. The default is `no`.
- `database.sql_type`: `mariadb` or `sqlite`. The default is `mariadb`.
  Existing config files that still use `mysql` must update this field.
- `database.host`, `database.port`, `database.user`, and `database.password`
  are only used for `mariadb`.
- `database.database`: for `mariadb`, this is the database name. For `sqlite`,
  this is the path to the database file.
- `logger.level`: one of `NOTSET`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, or
  `CRITICAL`.

---

## Q & A

- Why are some images missing from the CBZ-files?

`H2HDB` does not compress images that are considered spam according to certain
rules. If you encounter any images that you believe should have been included,
please report the issue.

- Why are some images in some CBZ files and not in other CBZ-files?

`H2HDB` learns the spam rule from the previous CBZ files. If you kill the CBZ
files containing these images, the new CBZ files will not contain these images.

---

## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

---

## License

This project is distributed under the terms of the GNU General Public Licence
(GPL). For detailed licence terms, see the `LICENSE` file included in this
distribution.
