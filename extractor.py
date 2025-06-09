import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

LIST_PAGE = "https://developer.imdb.com/non-commercial-datasets/"
WHERE_SAVE = r"./datasets/downloads"
CHUNK_SIZE = 1024 * 1024

def extract_page_file(page: str) -> tuple:
    resp = requests.get(page)
    html_content = resp.content

    soup = BeautifulSoup(html_content, 'html.parser')
    data_tag = soup.find('h2', id = 'data-location')

    p = data_tag.find_next_sibling('p')
    where_data = p.find('a')['href']

    h3_list = soup.find_all('h3')
    files_list = []
    
    for entry in h3_list:
        files_list.append(entry.text)

    return (where_data, files_list)

def download_files(page: str, files: list, sv_path: str):
    for file in files:
        download_url = f"{page}{file}"
        
        with requests.get(download_url, stream = True) as r:
            r.raise_for_status()
            full_dest = os.path.join(sv_path, file)

            total = int(r.headers.get('content-length', 0))

            with open(full_dest, 'wb') as f, tqdm(
                total = total, unit = 'iB', unit_scale = True, desc = full_dest
            ) as bar:
                for chunk in r.iter_content(chunk_size = CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
        
        print(f"Downloaded: {file}")

if __name__ == '__main__':
    where_data, files_list = extract_page_file(LIST_PAGE)
    print(f"Where the data is: {where_data}.\nWhich files are going to be: {files_list}")
    download_files(
        where_data,
        files_list,
        WHERE_SAVE
    )
