# 🏛️ Spark ETL – Kreiranje `Declaration_DIM` dimenzijske tablice

## 🎯 Cilj
Dimenzija **Declaration_DIM** predstavlja svaki službeni FEMA dekret (`declaration_request_number`).

| Stupac                      | Opis                                                  |
|-----------------------------|-------------------------------------------------------|
| `declaration_tk`            | Surrogate ključ (1 … n)                               |
| `version`                   | SCD‑verzija (trenutno 1)                              |
| `date_from`                 | Datum kad je red postao aktivan                       |
| `date_to`                   | Datum kraja (NULL = još aktivan)                      |
| `declaration_title`         | Naslov deklaracije                                    |
| `declaration_type`          | Vrsta (MAJOR DISASTER, EMERGENCY, …)                  |
| `declaration_date`          | Datum izdavanja                                       |
| `declaration_request_number`| **Prirodni ključ** – jedinstveni ID deklaracije       |

> Postoji i **UNKNOWN red** (`declaration_tk = 0`) za „nepovezane“ činjenice.

---

## 📂 Datoteka: `declaration_dim.py`

### 1. Unicode izlaz
```python
sys.stdout.reconfigure(encoding="utf-8")
```
Osluškuje da bi hrvatski znakovi (č, ć, ž, …) bili ispravno ispisani u terminalu.

---

### 2. Spark helper
```python
SparkSession.builder \
    .appName("US_DISASTER_SCHEMA") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```
- Četiri shuffle‑particije su optimalne za ~60k redova.

---

### 3. Funkcija `transform_declaration_dim()` – korak po korak

#### 3.1 *Extract* (baza + CSV)
```python
COLS = ["declaration_title", "declaration_type", "declaration_date", "declaration_request_number"]

db_df  = spark.read.jdbc(... '"Declaration"' ...) .select(*COLS)
csv_df = spark.read.csv(csv_path, header=True, inferSchema=True).select(*COLS)
```
- Čitamo **ista** četiri stupca iz oba izvora.

#### 3.2 Union + dedup
```python
all_decl = (db_df.unionByName(csv_df)
            .dropna(subset=["declaration_request_number"])
            .dropDuplicates(["declaration_request_number"])
            .withColumn("declaration_date", to_date(col("declaration_date"))))
```
- **`dropDuplicates`** jamči jedan red po prirodnom ključu (`declaration_request_number`).
- `to_date()` čisti potencijalni timestamp → čuvamo samo datum.

#### 3.3 Surrogate ključ + SCD kolone
```python
w = Window.orderBy("declaration_request_number")

dim = (all_decl
       .withColumn("declaration_tk", row_number().over(w))
       .withColumn("version", lit(1))
       .withColumn("date_from", current_timestamp())
       .withColumn("date_to", lit(None).cast("timestamp")))
```
- `row_number()` po `declaration_request_number` ⇒ deterministički `declaration_tk`.

#### 3.4 Fiksni redoslijed kolona
```python
COL_ORDER = ["declaration_tk", "version", "date_from", "date_to", *COLS]
dim = dim.select(*COL_ORDER)
```
- Pruža predvidljiv `schema` pri `overwrite` pisanju.

#### 3.5 UNKNOWN red
```python
unknown = (0,1,datetime.utcnow(),None,None,None,None,None)
unknown_df = spark.createDataFrame([unknown], schema=dim.schema)
final_df  = unknown_df.unionByName(dim)
```
- Red `0` hvata sve buduće činjenice koje nemaju valjan `declaration_request_number`.

---

### 4. Pisanje u bazu
```python
final_df.write.jdbc(url, "declaration_dim", mode="overwrite", properties=props)
```
- Prepisuje (overwrite) staru verziju dimenzije svaki put.

---

## 📊 Očekivani output
```text
DECLARATION_DIM redaka: ≈ 4 272   # 4 271 stvarnih + 1 UNKNOWN
```
Broj redova ovisi o tome koliko jedinstvenih deklaracija ima u bazi + 20 % CSV‑a.

---

## 📝 Best‑practice bilješke
1. Ako se u budućnosti za isti `declaration_request_number` promijeni `declaration_title` → pređi na SCD‑Type 2.
2. Datum može uključivati vrijeme; `to_date()` uklanja vrijeme jer ga ovdje ne trebamo.
3. Ne zaboravi indeksirati stupac `declaration_request_number` u bazi radi bržih join‑ova.

---

**Autor**: Data Engineering pomoćnik · lipanj 2025.

