import requests
from bs4 import BeautifulSoup
import pandas as pd

# STEP 1: Pick URL (practice site - legal, works)
url = "https://quotes.toscrape.com/"

# STEP 2: Download page
response = requests.get(url)

# STEP 3: Parse HTML
soup = BeautifulSoup(response.text, 'html.parser')

# STEP 4: Find data
quotes = []
for item in soup.find_all('div', class_='quote'):
    text = item.find('span', class_='text').get_text()
    author = item.find('small', class_='author').get_text()
    quotes.append({'quote': text, 'author': author})

# STEP 5: Save to Excel
df = pd.DataFrame(quotes)
df.to_excel('quotes.xlsx', index=False)

print(f"Saved {len(quotes)} quotes")