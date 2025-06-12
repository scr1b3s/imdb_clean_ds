from env import appEnv

import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from env import appEnv

SAVE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", appEnv.SAVE_DIR)
)


def extract_page_file(page: str, sv_path: str) -> list:
    resp = requests.get(page)
    html_content = resp.content

    soup = BeautifulSoup(html_content, "html.parser")
    data_tag = soup.find("h2", id="data-location")

    p = data_tag.find_next_sibling("p")
    where_data = p.find("a")["href"]

    h3_list = soup.find_all("h3")
    download_list = []

    for entry in h3_list:
        download_list.append((f"{where_data}{entry.text}", f"{sv_path}/{entry.text}"))

    return download_list


def download_files(url, path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=appEnv.CHUNK_SIZE):
                f.write(chunk)

        print(f"Downloaded: {path.split('/')[-1]}")


def download_many(file_list, max_workers=3):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda args: download_files(*args), file_list)


if __name__ == "__main__":
    download_list = extract_page_file(appEnv.LIST_PAGE, SAVE_DIR)

    for entry in download_list:
        print(entry)

    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    download_many(download_list)
