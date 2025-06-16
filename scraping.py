import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

base_url = "https://www.fema.gov"
start_url = "https://www.fema.gov/disaster/declarations"

headers = {
    "User-Agent": "Mozilla/5.0"
}

all_data = []

def extract_data(soup):
    cards = soup.find_all("div", class_="views-row")
    for card in cards:
        title_tag = card.find("a")
        if not title_tag:
            continue
        title = title_tag.text.strip()
        link = base_url + title_tag["href"]
        incident_period = next((s for s in card.stripped_strings if "Incident Period:" in s), "")
        declaration_date = next((s for s in card.stripped_strings if "Major Disaster Declaration declared" in s), "")


        all_data.append({
            "Title": title,
            "Link": link,
            "Incident Period": incident_period.strip() if incident_period else "",
            "Declaration Date": declaration_date.strip() if declaration_date else ""
        })

def get_all_pages():
    page = 0
    while True:
        url = f"{start_url}?page={page}"
        print(f"Scraping page {page + 1}...")

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")

        results = soup.find_all("div", class_="views-row")
        if not results:
            print("No more pages.")
            break

        extract_data(soup)
        page += 1
        time.sleep(1)  # Respectful delay

get_all_pages()

# Save to CSV
df = pd.DataFrame(all_data)
df.to_csv("fema_disaster_declarations.csv", index=False)
print("Done. Data saved to fema_disaster_declarations.csv")
