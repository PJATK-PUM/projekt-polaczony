# 🎓 Projekt: „Jedna domena – trzy modalności – jeden zautomatyzowany pipeline ML”

## Wspólny motyw danych

Przykładowe osie tematyczne (może wybrać jedną z nich, ale zapropnować własną):

- **Transport/Mobilność** (np. tabular: kursy taxi; obrazy: znaki drogowe + meta; duże: logi przejazdów)
- **E-commerce/Produkty** (tabular: koszyki/zakupy; obrazy: zdjęcia produktów + meta; duże: klik-stream/recenzje)
- **Zdrowie/Wellbeing** (tabular: pomiary/ankiety; obrazy: np. skany/zdjęcia z meta; duże: sygnały, np. audio tętna)
- **Media/Film/Muzyka** (tabular: ratingi; obrazy: plakaty + meta; duże: audio klipy)

### Wymogi dot. 3 zbiorów

1. **Tabular** – do 10 kolumn, ≤ 200 000 rekordów
2. **Obrazy + metadata** – co najmniej kilkaset obrazów + tabela z opisem (etykiety/cechy)
3. **„Duże dane” (wybór studenta)** – **jedno z**:
  - baza ≥ 1 000 000 rekordów **lub**
  - baza do trenowania LLM (np. teksty) **lub**
  - zbiory **wideo/audio** ≥ 5 000 próbek

---

## 🔧 Środowisko i repo (wymagane)

- **Airflow 2.x** (Docker Compose / Astronomer / MWAA — dowolnie)
- Python 3.10+; biblioteki: `pandas`, `scikit-learn`, `numpy`, `ydata-profiling`, `great-expectations`, `mlflow`, (dla obrazów: `torch` + `torchvision` lub `tensorflow`), (dla dużych: zależnie od wyboru, np. `dask`/`pyspark`/`datasets`)
- **MLflow** do metryk/artefaktów
- **Great Expectations** do Data Quality
- Repozytorium **GitHub** (obowiązkowo: `README`, `/dags`, `/include/config`, `/src`, `/notebooks`, `/reports`, `/mlruns` (lub zewn. tracking))

Przykładowa struktura:

```
.
├─ dags/
│  ├─ dag_ingest_eda_prep.py
│  ├─ dag_automl.py
│  └─ dag_retrain_stage2.py
├─ include/
│  └─ config/
│     ├─ tabular.yaml
│     ├─ images.yaml
│     └─ bigdata.yaml
├─ src/
│  ├─ io/        # pobieranie/ładowanie
│  ├─ dq/        # checks, GE suites
│  ├─ eda/       # raporty profilujące
│  ├─ prep/      # feature pipelines
│  ├─ models/    # trening/inferencja
│  └─ utils/
├─ reports/      # wygenerowane raporty (HTML/MD)
├─ notebooks/    # szkice/eksploracja
├─ requirements.txt
└─ README.md
```

---

# 🗓️ Plan 4 zajęć

## 🧩 Zajęcia 1 — **Airflow: automatyczny ingest → quality → EDA → przygotowanie**

**Cel:** zbudować w pełni automatyczny **pierwszy etap pipeline’u** dla 3 zbiorów.

### Zakres:

- Wybór **wspólnej domeny** i **3 zbiorów** zgodnych z wymogami.
- Konfiguracja **Airflow** + repo GitHub.
- **DAG #1 (ingest_eda_prep)** per zbiór:
  1. **Ingest**: automatyczne pobranie danych (HTTP/Cloud/Local), walidacja schematu.
  2. **Data Quality**: **Great Expectations** – min. 5 asercji (np. brak duplikatów klucza, typy, zakresy, brak NA w target, procent braków < X%).
  3. **EDA**: szybki raport (`ydata-profiling`) zapisywany do `/reports`.
  4. **Prep**: skrypty do featuringu:
    - tabular: imputacja, encode, scale; zapis „clean.parquet”
    - obrazy: weryfikacja metadanych, konsolidacja manifestu, podział train/val, proste augmentacje
    - duże: streaming/partycjonowanie, filtracja outliers, zapis do formatu kolumnowego (Parquet) lub shardów

---

## 🤖 Zajęcia 2 — **AutoML w Airflow: wybierz i zbuduj TOP-3 modele per zbiór**

**Cel:** w pełni automatyczny **trening wielu modeli** i wybór **3 najlepszych** dla każdego z 3 zbiorów.

### Zakres:

- **DAG #2 (automl)** zależny od artefaktów z DAG #1.
- Dla **tabular**: baseline (Logistic/Linear), drzewne (RandomForest/XGBoost/LightGBM), prosty MLP.
- Dla **obrazów**: transfer learning (np. ResNet/MobileNet/EfficientNet, 5–10 epok), zapis wag.
- Dla **dużych danych**:
  - jeśli tabular → algorytmy skalowalne (Dask/XGBoost on Dask),
  - jeśli LLM → klasyfikacja/fine-tuning adapterów (LoRA) na małym modelu,
  - jeśli audio/wideo → ekstrakcja cech (MFCC/CLIP embeddings) + klasyfikator.
- **MLflow**: rejestr metryk i artefaktów; selekcja **TOP-3** po zadanej metryce (np. F1/ROC-AUC/MAP).

---

## 🔁 Zajęcia 3 — **Dzielimy dane na pół: model bazowy (50%) + dokształcanie (50%)**

**Cel:** pokazać **uczenie przyrostowe/transfer/fine-tuning** jako praktykę produkcyjną.

### Reguła:

- Automatyczne pobranie danych (HTTP/Cloud/Local), walidacja schematu.
- Każdy zbiór dzielimy na **część A (50%)** i **część B (50%)** (z zachowaniem rozkładów/stratified).
- Przygotowanie danych danych z części A,
- Trenujemy **model bazowy** na A, następnie **dokształcamy** na B.

### Wzorce implementacyjne:

- **Tabular**: modele z `partial_fit` (SGDClassifier/Regressor, Perceptron), ewentualnie **XGBoost** z „continued training” na dodatkowych rundach; lub **warm_start** w drzewach (kontrolowana przebudowa).
- **Obrazy**: transfer learning – najpierw zamrażamy backbone i trenujemy head (A), potem odmrażamy kilka warstw i fine-tuning na B (mała LR, early stopping).
- **Duże dane**:
  - tabular → mini-batch (Dask),
  - LLM → adapter (LoRA) najpierw na A, potem krótkie dociągnięcie na B,
  - audio/wideo → najpierw embedding model + liniowy klasyfikator, potem doczytanie embeddingów z B i dalsze uczenie.

---

## 🧾 Zajęcia 4 — **Raport i prezentacja (max 4 min)**

**Cel:** zsyntetyzować wyniki, pokazać decyzje inżynierskie i wnioski.

### Wymogi:

- **Automatyczny raport** (skrypt generujący Markdown/HTML z metryk MLflow + mini-wykresy), np. `python src/utils/make_report.py`.
- Struktura raportu (1–2 strony):
  1. Problem i motyw danych (jedno zdanie)
  2. Skąd dane? Jak spełniają wymogi 3 zbiorów?
  3. Najlepsze 3 modele per zbiór (metryki, krótki komentarz)
  4. Efekt dokształcania (co się poprawiło, kiedy warto, koszty)
  5. Co byśmy zrobili dalej (1–2 hipotezy)
- **Prezentacja 4 min (twarde ograniczenie)** – po 5:00 **niezaliczona**.
  - Zalecany format: 2 slajdy + live pokaz grafów MLflow lub widoku Airflow.
  - Mówimy o **wnioskach**, nie o slajdach.

