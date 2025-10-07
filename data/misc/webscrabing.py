import requests
from bs4 import BeautifulSoup

# Target website
url = "https://www.bbc.com/news"

# Send HTTP GET request
response = requests.get(url)
response.raise_for_status()  # stop if there's an error

# Parse HTML content
soup = BeautifulSoup(response.text, "lxml")
print (soup.title.string)  # print page title

# Extract news headlines (using HTML tags and classes)
headlines = soup.find_all("h2")

print("📰 Latest BBC News Headlines:\n")
for i, headline in enumerate(headlines[:10], start=1):  # limit to top 10
    text = headline.get_text(strip=True)
    if text:
        print(f"{i}. {text}")

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ===========================================================================----------------------------
# SETTINGS
# ----------------------------
BASE_URL = "https://mse.co.mw/market/reports"   # ← replace with real page
DOWNLOAD_DIR = "D:\Documents\AIMS_DSCBI_Training\mse-api-assignment\data\misc\downloads"               # local folder name
FILE_TYPES = [".pdf"]   # which files to download

# ----------------------------
# CREATE DOWNLOAD FOLDER
# ----------------------------
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ----------------------------
# FETCH PAGE CONTENT
# ----------------------------
print(f"Fetching {BASE_URL} ...")
response = requests.get(BASE_URL)
response.raise_for_status()

# ----------------------------
# PARSE HTML
# ----------------------------
soup = BeautifulSoup(response.text, "html.parser")
links = soup.find_all("a")

# ----------------------------
# LOOP THROUGH LINKS
# ----------------------------
for link in links:
    href = link.get("href")
    if href and any(href.endswith(ext) for ext in FILE_TYPES):
        file_url = urljoin(BASE_URL, href)
        file_name = os.path.basename(file_url)
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        print(f"📥 Downloading: {file_name} ...")

        # Download file
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

print("\n✅ All files downloaded successfully!")
