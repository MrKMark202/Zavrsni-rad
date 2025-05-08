import pandas as pd

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "data/us_disaster_declarations.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',', dtype={'fips': str})
print("CSV size before: ", df.shape)

# Izdvoji samo stupac 'declaration_request_number'
df_only_requests = df[['declaration_request_number']].copy()

# Spremi u novi CSV fajl
df_only_requests.to_csv("declaration_requests_only.csv", index=False)

print("Novi CSV fajl 'declaration_requests_only.csv' je uspješno kreiran.")
