## 🧩 Zajęcia 1 — **Airflow: automatyczny ingest → quality → EDA → przygotowanie ( 20 punktów )**

**Cel:** zbudować w pełni automatyczny **pierwszy etap pipeline’u** dla 3 zbiorów.

### Zakres:

* Wybór **wspólnej domeny** i **3 zbiorów** zgodnych z wymogami.
* Konfiguracja **Airflow** + repo GitHub.
* **DAG #1 (ingest_eda_prep)** per zbiór:

  1. **Ingest**: automatyczne pobranie danych (HTTP/Cloud/Local), walidacja schematu.
  2. **Data Quality**: **(np. Great Expectations)** – min. 5 asercji (np. brak duplikatów klucza, typy, zakresy, brak NA w target, procent braków < X%).
  3. **EDA**: szybki raport (`ydata-profiling`) zapisywany do `/reports`.
  4. **Prep**: skrypty do featuringu:

     * tabular: imputacja, encode, scale; zapis „clean.parquet”
     * obrazy: weryfikacja metadanych, konsolidacja manifestu, podział train/val, proste augmentacje
     * duże: streaming/partycjonowanie, filtracja outliers, zapis do formatu kolumnowego (Parquet) lub shardów

### Szkic DAG (TaskFlow API, skrótowo)

```python
from airflow.decorators import dag, task
from pendulum import datetime
from src.io import ingest_tabular, ingest_images, ingest_big
from src.dq import run_ge_suite
from src.eda import profile_dataset
from src.prep import prep_tabular, prep_images, prep_big

@dag(schedule="@daily", start_date=datetime(2025, 11, 1), catchup=False, tags=["ingest","eda","prep"])
def ingest_eda_prep():
    for ds in ["tabular","images","big"]:
        data = ingest(ds)          # pobierz wg include/config/{ds}.yaml
        dq = run_ge_suite(ds, data)
        eda = profile_dataset(ds, data)
        clean = prep(ds, data)
        dq >> eda >> clean
    # fanout 3 x ścieżka
ingest_eda_prep()
```

> W `include/config/*.yaml` trzymamy źródła, kolumny obowiązkowe, progi jakości.

### „Checkpoint” po zajęciach 1:

* Działający **DAG #1** dla 3 zbiorów (ręczne uruchomienie OK).
* Raporty EDA i logi Quality w repo.
* Krótka sekcja w README: opis domeny i źródeł.
