import pandas as pd

# Učitaj 80% podataka
df_80 = pd.read_csv("US_DISASTERS_PROCESSED_20.csv")

# Filtriraj redove gdje je declaration_denials == 1
denied_requests_80 = df_80[df_80['declaration_denials'] == 1]

# Ispis broja i podataka
print(f"Broj odbijenih zahtjeva (80% skup): {len(denied_requests_80)}")
print(denied_requests_80)

# Po želji, spremi u novi CSV
denied_requests_80.to_csv("US_DISASTERS_DECLARATION_DENIED_80.csv", index=False)
