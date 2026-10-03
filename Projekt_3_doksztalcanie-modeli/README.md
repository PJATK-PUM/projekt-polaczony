## 🔁 Zajęcia 3 — **Dzielimy dane na pół: model bazowy (50%) + dokształcanie (50%)**

**Cel:** pokazać **uczenie przyrostowe/transfer/fine-tuning** jako praktykę produkcyjną.

### Reguła:

* Automatyczne pobranie danych (HTTP/Cloud/Local), walidacja schematu.
* Każdy zbiór dzielimy na **część A (50%)** i **część B (50%)** (z zachowaniem rozkładów/stratified).
* Przygotowanie danych danych z części A,
* Trenujemy **model bazowy** na A, następnie **dokształcamy** na B.

### Wzorce implementacyjne:

* **Tabular**: modele z `partial_fit` (SGDClassifier/Regressor, Perceptron), ewentualnie **XGBoost** z „continued training” na dodatkowych rundach; lub **warm_start** w drzewach (kontrolowana przebudowa).
* **Obrazy**: transfer learning – najpierw zamrażamy backbone i trenujemy head (A), potem odmrażamy kilka warstw i fine-tuning na B (mała LR, early stopping).
* **Duże dane**:

  * tabular → mini-batch (Dask),
  * LLM → adapter (LoRA) najpierw na A, potem krótkie dociągnięcie na B,
  * audio/wideo → najpierw embedding model + liniowy klasyfikator, potem doczytanie embeddingów z B i dalsze uczenie.

### DAG #3 (retrain_stage2)

```python
@dag(schedule=None, start_date=datetime(2025,11,1), catchup=False, tags=["retrain"])
def retrain_stage2():
    for ds in ["tabular","images","big"]:
        base = train_on_split(ds, split="A")     # bazowy
        incr = continue_training(ds, base, split="B")  # dokształcanie
        eval = evaluate_model(ds, incr)          # porównanie vs bazowy
        base >> incr >> eval
retrain_stage2()
```

**Wymagane artefakty:**

* porównanie metryk „bazowy vs dokształcony”, wykresy uczenia, confusion matrix/PR/ROC (w zależności od zadania), opisy co zmieniło się po dociąganiu.

### „Checkpoint” po zajęciach 3:

* Działający **DAG #3** i artefakty ewaluacyjne w MLflow.
* W repo: `retraining_findings.md` z wnioskami.
