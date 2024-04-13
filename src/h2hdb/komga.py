# swagger-ui/index.html
import requests  # type: ignore
from requests.auth import HTTPBasicAuth  # type: ignore

from .logger import logger


def get_series_ids(
    library_id: str, base_url: str, api_username: str, api_password: str
) -> list[str]:
    series_informations = list[tuple[str, str]]()
    page_num = 0
    while True:
        logger.debug(f"Getting series page {page_num} for library {library_id}")
        url = (
            f"{base_url}/api/v1/series?library_id={library_id}&page={page_num}&size=100"
        )
        response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
        response.raise_for_status()
        response_json = response.json()
        if len(response_json["content"]) == 0:
            break
        for series in response_json["content"]:
            series_informations.append((series["id"], series["fileLastModified"]))
        page_num += 1
    series_ids = list({s[0] for s in sorted(series_informations, key=lambda x: x[1])})
    return series_ids


def get_books_ids(
    series_id: str, base_url: str, api_username: str, api_password: str
) -> list[str]:
    books_informations = list[tuple[str, str]]()
    page_num = 0
    while True:
        logger.debug(f"Getting books page {page_num} for series {series_id}")
        url = f"{base_url}/api/v1/series/{series_id}/books?page={page_num}&size=100"
        response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
        response.raise_for_status()
        response_json = response.json()
        if len(response_json["content"]) == 0:
            break
        for book in response_json["content"]:
            books_informations.append((book["id"], book["fileLastModified"]))
        page_num += 1
    books_ids = list({b[0] for b in sorted(books_informations, key=lambda x: x[1])})
    return books_ids


def get_book(book_id: str, base_url: str, api_username: str, api_password: str) -> dict:
    url = f"{base_url}/api/v1/books/{book_id}"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.json()


def patch_book_metadata(
    metadata: dict, book_id: str, base_url: str, api_username: str, api_password: str
) -> None:
    url = f"{base_url}/api/v1/books/{book_id}/metadata"
    response = requests.patch(
        url,
        json=metadata,
        auth=HTTPBasicAuth(api_username, api_password),
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(response.content)
        logger.error(f"Error while patching book metadata: {e} {book_id}")
        patch_book_metadata(metadata, book_id, base_url, api_username, api_password)


def download_book(
    book_id: str, base_url: str, api_username: str, api_password: str
) -> bytes:
    url = f"{base_url}/api/v1/books/{book_id}/file"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.content


def get_series(
    series_id: str, base_url: str, api_username: str, api_password: str
) -> dict:
    url = f"{base_url}/api/v1/series/{series_id}"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.json()


def patch_series_metadata(
    metadata: dict, series_id: str, base_url: str, api_username: str, api_password: str
) -> None:
    url = f"{base_url}/api/v1/series/{series_id}/metadata"
    response = requests.patch(
        url,
        json=metadata,
        auth=HTTPBasicAuth(api_username, api_password),
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(response.content)
        logger.error(f"Error while patching series metadata: {e} {series_id}")
        patch_series_metadata(metadata, series_id, base_url, api_username, api_password)
