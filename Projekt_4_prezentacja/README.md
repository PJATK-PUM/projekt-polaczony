## 🧾 Zajęcia 4 — **Raport i prezentacja (max 4 min)**

**Cel:** zsyntetyzować wyniki, pokazać decyzje inżynierskie i wnioski.

### Wymogi:

* **Automatyczny raport** (skrypt generujący Markdown/HTML z metryk MLflow + mini-wykresy), np. `python src/utils/make_report.py`.
* Struktura raportu (1–2 strony):

  1. Problem i motyw danych (jedno zdanie)
  2. Skąd dane? Jak spełniają wymogi 3 zbiorów?
  3. Najlepsze 3 modele per zbiór (metryki, krótki komentarz)
  4. Efekt dokształcania (co się poprawiło, kiedy warto, koszty)
  5. Co byśmy zrobili dalej (1–2 hipotezy)
* **Prezentacja 4 min (twarde ograniczenie)** – po 5:00 **niezaliczona**.

  * Zalecany format: 2 slajdy + live pokaz grafów MLflow lub widoku Airflow.
  * Mówimy o **wnioskach**, nie o slajdach.

### „Checkpoint” po zajęciach 4:

* Raport w `/reports` i w README (link).
* Prezentacja przećwiczona (timer), fokus na **wnioski i decyzje**.

---

# 🧠 Wymuszone samokształcenie (jak „podkręcić śrubę”)

* **Konfiguracja-first:** wszystko sterowane plikami `*.yaml` (źródła, metryki, progi, hyperparam search space).
* **Eksperymenty:** każdy model musi mieć **co najmniej 2 warianty** hiperparametrów, uzasadnionych w raporcie.
* **Skalowanie:** w „dużych danych” zespół **musi** pokazać albo batching/partycjonowanie, albo wybór narzędzia (Dask/PySpark) z krótkim uzasadnieniem.
* **Repro:** `make up && make flow` uruchamia cały pipeline (Makefile).
* **Czystość repo:** PR-y, code review, `pre-commit` (flake8/black/isort).

---

## ✍️ Minimalne szkielety funkcji (do wklejenia do `src/…`)

**Great Expectations – przykładowe asercje (tabular):**

```python
def basic_ge_suite_expectations(df):
    from great_expectations.dataset import PandasDataset
    p = PandasDataset(df)
    p.expect_table_row_count_to_be_between(min_value=100)
    p.expect_column_values_to_not_be_null("target")
    p.expect_column_values_to_be_of_type("age","float")
    p.expect_column_values_to_be_between("age", min_value=0, max_value=120)
    p.expect_column_values_to_be_unique("id")
    return p.validate()
```

**AutoML – selekcja TOP-3 (tabular, skrót):**

```python
def run_tabular_automl(X_train, y_train, X_val, y_val, experiment_name):
    import mlflow, numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from xgboost import XGBClassifier
    models = {
        "logreg": LogisticRegression(max_iter=1000),
        "rf":     RandomForestClassifier(n_estimators=300),
        "xgb":    XGBClassifier(n_estimators=400, max_depth=6, tree_method="hist")
    }
    results = []
    for name, model in models.items():
        with mlflow.start_run(run_name=name, experiment_id=mlflow.create_experiment(experiment_name)):
            model.fit(X_train, y_train)
            f1 = f1_score(y_val, model.predict(X_val))
            mlflow.log_metric("f1", f1)
            mlflow.sklearn.log_model(model, "model")
            results.append((name, f1))
    return sorted(results, key=lambda t: t[1], reverse=True)[:3]
```

**Dokształcanie – dwa etapy (tabular):**

```python
def two_stage_training(XA, yA, XB, yB):
    from sklearn.linear_model import SGDClassifier
    clf = SGDClassifier(loss="log_loss", max_iter=5)
    clf.partial_fit(XA, yA, classes=np.unique(yA))
    # stage 2
    clf.partial_fit(XB, yB)
    return clf
```

**Obrazy – transfer + fine-tuning (pseudokod):**

```python
# stage A: freeze backbone, train classifier head
# stage B: unfreeze last N layers, low LR fine-tune on split B with early stopping
```

---

## ✅ Co dostarczają studenci po każdym etapie

* **Z1:** działający DAG #1 + raporty EDA + GE validation + opis domeny
* **Z2:** działający DAG #2 + MLflow (TOP-3 per zbiór) + `models_results.md`
* **Z3:** działający DAG #3 + porównania bazowy vs dokształcony + `retraining_findings.md`
* **Z4:** raport końcowy + 4-min prezentacja (twardy limit)

---

Jeśli chcesz, mogę od razu:

1. wygenerować **gotowe szkielety trzech DAG-ów** (do skopiowania do `/dags`),
2. dorzucić **przykładowe `*.yaml`** (konfiguracje per zbiór),
3. przygotować **checklistę prowadzącego** do szybkiego sprawdzania postępów.
