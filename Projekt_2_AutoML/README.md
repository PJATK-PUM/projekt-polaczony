## 🤖 Zajęcia 2 — **AutoML w Airflow: wybierz i zbuduj TOP-3 modele per zbiór**

**Cel:** w pełni automatyczny **trening wielu modeli** i wybór **3 najlepszych** dla każdego z 3 zbiorów.

### Zakres:

* **DAG #2 (automl)** zależny od artefaktów z DAG #1.
* Dla **tabular**: baseline (Logistic/Linear), drzewne (RandomForest/XGBoost/LightGBM), prosty MLP.
* Dla **obrazów**: transfer learning (np. ResNet/MobileNet/EfficientNet, 5–10 epok), zapis wag.
* Dla **dużych danych**:

  * jeśli tabular → algorytmy skalowalne (Dask/XGBoost on Dask),
  * jeśli LLM → klasyfikacja/fine-tuning adapterów (LoRA) na małym modelu,
  * jeśli audio/wideo → ekstrakcja cech (MFCC/CLIP embeddings) + klasyfikator.
* **MLflow**: rejestr metryk i artefaktów; selekcja **TOP-3** po zadanej metryce (np. F1/ROC-AUC/MAP).

### Szkic DAG (równoległy trening + selekcja)

```python
@dag(schedule=None, start_date=datetime(2025,11,1), catchup=False, tags=["automl"])
def automl_train():
    for ds in ["tabular","images","big"]:
        runs = launch_model_sweep.expand(
            ds=[ds], 
            model_family=[ "baseline","tree","boost","mlp" ] if ds=="tabular" 
                         else [ "resnet","mobilenet","efficientnet" ] if ds=="images"
                         else [ "scalable_tab","adapter_llm","embed_cls" ]
        )  # dynamic task mapping
        select_top3(ds, upstream_task_ids=runs)
automl_train()
```

### „Checkpoint” po zajęciach 2:

* Uruchomiony **DAG #2** na 3 zbiorach.
* W MLflow: listy eksperymentów, metryki, artefakty.
* W repo: `models_results.md` z tabelą TOP-3 per zbiór (autogenerowane).
