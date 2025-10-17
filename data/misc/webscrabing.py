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
DOWNLOAD_DIR = "mse_reports"               # local folder name
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


# Find all PDF links
pdf_links = []
for link in soup.find_all("a", href=True):
    href = link["href"]
    if href.lower().endswith(".pdf"):
        full_url = urljoin(BASE_URL, href)
        pdf_links.append(full_url)

# Download PDFs
if not pdf_links:
    print("⚠️ No PDF links found. Check if the page structure has changed.")
else:
    print(f"Found {len(pdf_links)} PDF files. Starting download...\n")

for i, pdf_url in enumerate(pdf_links, start=1):
    file_name = os.path.basename(pdf_url.split("?")[0])  # remove query params
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    print(f"📥 ({i}/{len(pdf_links)}) Downloading: {file_name} ...")
    r = requests.get(pdf_url, stream=True)
    r.raise_for_status()

    with open(file_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

print("\n✅ All MSE daily reports downloaded successfully!")
print(f"Saved in: {os.path.abspath(DOWNLOAD_DIR)}")
