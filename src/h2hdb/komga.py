# swagger-ui/index.html
import requests  # type: ignore
from requests.auth import HTTPBasicAuth  # type: ignore
from time import sleep

from .logger import logger


def retry_request(request, retries: int = 3):
    def wrapper(*args, **kwargs):
        if retries < 0:
            logger.error("Exceeded maximum retries. Aborting.")
            return
        else:
            try:
                return request(*args, **kwargs)
            except requests.exceptions.SSLError:
                logger.error("SSL error while making request. Need to update certifi.")
            except requests.exceptions.RequestException as e:
                retry_codes = [
                    "500",
                    "504",
                    "429",
                ]  # Add more codes to this list as needed
                if any(code in str(e) for code in retry_codes):
                    logger.warning(
                        f"Encountered error {str(e)}. Retrying in 5 seconds."
                    )
                    sleep(5)
                    return retry_request(request, retries - 1)(*args, **kwargs)
                elif "401" in str(e):
                    logger.error(
                        f"Unauthorized error while making request. Check your credentials."
                    )
                    return  # Don't retry
                else:
                    logger.error(f"Error while making request: {e}")
                    return  # Don't retry

    return wrapper


@retry_request
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


@retry_request
def get_books_ids_in_series_id(
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


@retry_request
def get_books_ids_in_library_id(
    library_id: str, base_url: str, api_username: str, api_password: str
) -> list[str]:
    books_informations = list[tuple[str, str]]()
    page_num = 0
    while True:
        logger.debug(f"Getting books page {page_num} for library {library_id}")
        url = (
            f"{base_url}/api/v1/books?library_id={library_id}&page={page_num}&size=100"
        )
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


@retry_request
def get_books_ids_in_all_libraries(
    base_url: str, api_username: str, api_password: str
) -> list[str]:
    books_informations = list[tuple[str, str]]()
    page_num = 0
    while True:
        logger.debug(f"Getting books page {page_num} for all libraries")
        url = f"{base_url}/api/v1/books?page={page_num}&size=100"
        response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
        response.raise_for_status()
        response_json = response.json()
        if len(response_json["content"]) == 0:
            break
        for book in response_json["content"]:
            books_informations.append((book["id"], book["fileLastModified"]))
        page_num += 1
    # Sort by fileLastModified in descending order
    books_ids = list(
        {b[0] for b in sorted(books_informations, key=lambda x: x[1], reverse=True)}
    )
    return books_ids


@retry_request
def get_book(book_id: str, base_url: str, api_username: str, api_password: str) -> dict:
    url = f"{base_url}/api/v1/books/{book_id}"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.json()


@retry_request
def patch_book_metadata(
    metadata: dict, book_id: str, base_url: str, api_username: str, api_password: str
) -> None:
    url = f"{base_url}/api/v1/books/{book_id}/metadata"
    response = requests.patch(
        url,
        json=metadata,
        auth=HTTPBasicAuth(api_username, api_password),
    )
    response.raise_for_status()


@retry_request
def download_book(
    book_id: str, base_url: str, api_username: str, api_password: str
) -> bytes:
    url = f"{base_url}/api/v1/books/{book_id}/file"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.content


@retry_request
def scan_library(
    library_id: str, base_url: str, api_username: str, api_password: str
) -> None:
    url = f"{base_url}/api/v1/libraries/{library_id}/scan"
    response = requests.post(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()


@retry_request
def get_series(
    series_id: str, base_url: str, api_username: str, api_password: str
) -> dict:
    url = f"{base_url}/api/v1/series/{series_id}"
    response = requests.get(url, auth=HTTPBasicAuth(api_username, api_password))
    response.raise_for_status()
    return response.json()


@retry_request
def patch_series_metadata(
    metadata: dict, series_id: str, base_url: str, api_username: str, api_password: str
) -> None:
    url = f"{base_url}/api/v1/series/{series_id}/metadata"
    response = requests.patch(
        url,
        json=metadata,
        auth=HTTPBasicAuth(api_username, api_password),
    )
    response.raise_for_status()
