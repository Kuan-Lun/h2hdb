# ComicDB
## Description

H2HDB is a database for managing gallery collections from H@H. It allows users to keep track of their galleries, including details such as GID, title, tags, and more.

## Features

- [x] Add new galleries to the database
- [] Edit existing gallery details
- [] View a list of all galleries in the collection
- [] Delete galleries from the database
- [] Record the removed GIDs in a separate list
- [] Backup and restore the database
- [] Export the database to a CSV file
- [] Import a database from a CSV file

### Tables

| Name            | Comment                         |
| :-------------- | :------------------------------ |
| galleries_names | The downloaded gallery from H@H |
|

## Installation

1. Install Python 3.12 or higher from [python.org](https://www.python.org/downloads/).
2. Clone the repository.
    ```bash
    git clone [uri]
    ```
3. Install the required packages by running `pip install -r requirements.txt`.
4. Run the script by running `python comicdb.py`.

## Usage

1. Open the script by running `python comicdb.py`.
2. Follow the on-screen instructions to navigate the database.

## License

The project is licensed under the MIT license. For more information, see the `LICENSE` file.

## Credits

The project was created by [Kuan-Lun Wang](https://www.klwang.tw/home/).

## License

The project is licensed under the MIT license. For more information, see the `LICENSE` file.