package es.upv.pros.mlgenerator;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

public class PythonGenerator {

  private static final Set<String> ALGORITHMS = Set.of(
      "Logistic Regression",
      "Decision Trees",
      "Random Forest",
      "SVM",
      "Neural Networks",
      "K-Means",
      "Hierarchical Clustering",
      "DBSCAN"
  );

  private static final Set<String> METRICS = Set.of(
      "accuracy", "precision", "f1", "roc_auc", "confusion_matrix",
      "mae", "mse", "rmse",
      "silhouette", "davies_bouldin", "calinski_harabasz",
      "anomaly_rate"
  );

  public static String generatePython(JsonNode modelsNode, JsonNode datasetsNode) {
    if (modelsNode == null || modelsNode.isNull() || !modelsNode.isObject()) {
      throw new IllegalArgumentException("'models' debe ser un objeto (map nombreModelo -> spec).");
    }
    if (datasetsNode == null || datasetsNode.isNull() || !datasetsNode.isObject()) {
      throw new IllegalArgumentException("'dataset' debe existir y ser un objeto para usar train-ratio/val-ratio/test-ratio.");
    }

    ImportPlan plan = analyze(modelsNode);

    LinkedHashSet<String> imports = new LinkedHashSet<>();
    imports.add("from __future__ import annotations");
    imports.add("from typing import Dict, Any, Tuple, Optional, List");
    imports.add("import os");
    imports.add("import time");
    imports.add("import argparse");
    imports.add("import numpy as np");
    imports.add("import pandas as pd");
    imports.add("import joblib");
    imports.add("from sklearn.model_selection import train_test_split");

    imports.addAll(plan.estimatorImports);

    if (!plan.metricImports.isEmpty()) {
      imports.add("from sklearn.metrics import " + String.join(", ", plan.metricImports));
    }

    if (plan.needsScheduler) {
      imports.add("from apscheduler.schedulers.background import BackgroundScheduler");
    }

    if (plan.needsRest) {
      imports.add("from fastapi import FastAPI, HTTPException");
      imports.add("import uvicorn");
    }

    StringBuilder py = new StringBuilder();
    for (String imp : imports) {
      py.append(imp).append("\n");
    }
    py.append("\n");

    py.append("# --- Preprocessing (one-hot) & CSV loader ---\n");
    py.append("def preprocess_X(df: pd.DataFrame, feature_columns: Optional[List[str]] = None) -> Tuple[pd.DataFrame, List[str]]:\n");
    py.append("    X = pd.get_dummies(df, drop_first=True)\n");
    py.append("    X = X.replace([np.inf, -np.inf], np.nan).fillna(0)\n");
    py.append("    if feature_columns is None:\n");
    py.append("        feature_columns = list(X.columns)\n");
    py.append("        return X, feature_columns\n");
    py.append("    X = X.reindex(columns=feature_columns, fill_value=0)\n");
    py.append("    return X, feature_columns\n\n");

    py.append("def load_dataset(csv_path: str, task_type: str) -> Tuple[pd.DataFrame, Optional[pd.Series], List[str]]:\n");
    py.append("    \"\"\"Load dataset from CSV.\n");
    py.append("    For Classification/Regression returns X, y, feature_columns.\n");
    py.append("    For Clustering returns X, None, feature_columns.\n");
    py.append("    Target column heuristic: 'target', else 'label', else last column.\n");
    py.append("    \"\"\"\n");
    py.append("    df = pd.read_csv(csv_path)\n");
    py.append("    if df.shape[1] < 1:\n");
    py.append("        raise ValueError(f\"Empty dataset: {csv_path}\")\n\n");

    py.append("    if task_type == 'Clustering':\n");
    py.append("        X, cols = preprocess_X(df, None)\n");
    py.append("        return X, None, cols\n\n");

    py.append("    if 'target' in df.columns:\n");
    py.append("        y = df['target']\n");
    py.append("        Xraw = df.drop(columns=['target'])\n");
    py.append("    elif 'label' in df.columns:\n");
    py.append("        y = df['label']\n");
    py.append("        Xraw = df.drop(columns=['label'])\n");
    py.append("    else:\n");
    py.append("        y = df.iloc[:, -1]\n");
    py.append("        Xraw = df.iloc[:, :-1]\n\n");

    py.append("    X, cols = preprocess_X(Xraw, None)\n");
    py.append("    return X, y, cols\n\n");

    py.append("# --- Dataset override support ---\n");
    py.append("def parse_dataset_overrides(dataset_args: Optional[List[str]]) -> Dict[str, str]:\n");
    py.append("    overrides: Dict[str, str] = {}\n");
    py.append("    if not dataset_args:\n");
    py.append("        return overrides\n");
    py.append("    for raw in dataset_args:\n");
    py.append("        if '=' in raw:\n");
    py.append("            model_name, path = raw.split('=', 1)\n");
    py.append("            model_name = model_name.strip()\n");
    py.append("            path = path.strip()\n");
    py.append("            if not model_name or not path:\n");
    py.append("                raise ValueError(f\"Invalid --dataset override: {raw}\")\n");
    py.append("            overrides[model_name] = path\n");
    py.append("        else:\n");
    py.append("            raw = raw.strip()\n");
    py.append("            if not raw:\n");
    py.append("                raise ValueError(\"--dataset cannot be empty\")\n");
    py.append("            overrides['__default__'] = raw\n");
    py.append("    return overrides\n\n");

    py.append("def resolve_dataset_for_model(model_name: str, spec: Dict[str, Any], dataset_overrides: Optional[Dict[str, str]] = None) -> str:\n");
    py.append("    dataset_overrides = dataset_overrides or {}\n");
    py.append("    if model_name in dataset_overrides:\n");
    py.append("        return dataset_overrides[model_name]\n");
    py.append("    if '__default__' in dataset_overrides:\n");
    py.append("        return dataset_overrides['__default__']\n");
    py.append("    return spec['dataset']\n\n");

    py.append("# --- Dataset split specifications from DSL ---\n");
    py.append("def dataset_specs() -> Dict[str, Dict[str, float]]:\n");
    py.append("    specs: Dict[str, Dict[str, float]] = {}\n\n");

    Set<String> emittedDatasets = new LinkedHashSet<>();
    Iterator<String> modelNamesForDatasets = modelsNode.fieldNames();
    while (modelNamesForDatasets.hasNext()) {
      String modelName = modelNamesForDatasets.next();
      JsonNode modelSpec = modelsNode.get(modelName);
      String datasetName = requireText(modelSpec, "dataset");

      if (!emittedDatasets.add(datasetName)) {
        continue;
      }

      DatasetSplitSpec split = readDatasetSplitSpec(datasetName, datasetsNode);

      py.append("    specs['").append(escapePy(datasetName)).append("'] = {\n");
      py.append("        'train_ratio': ").append(pythonFloat(split.train)).append(",\n");
      py.append("        'val_ratio': ").append(pythonFloat(split.val)).append(",\n");
      py.append("        'test_ratio': ").append(pythonFloat(split.test)).append("\n");
      py.append("    }\n\n");
    }
    py.append("    return specs\n\n");

    py.append("# --- Model artifacts ---\n");
    py.append("def save_artifact(model_name: str, model: Any, feature_columns: List[str], out_dir: str) -> str:\n");
    py.append("    os.makedirs(out_dir, exist_ok=True)\n");
    py.append("    path = os.path.join(out_dir, f\"{model_name}.joblib\")\n");
    py.append("    joblib.dump({'model': model, 'feature_columns': feature_columns}, path)\n");
    py.append("    return path\n\n");

    py.append("def load_artifact(model_name: str, out_dir: str) -> Tuple[Any, List[str]]:\n");
    py.append("    path = os.path.join(out_dir, f\"{model_name}.joblib\")\n");
    py.append("    payload = joblib.load(path)\n");
    py.append("    return payload['model'], payload['feature_columns']\n\n");

    py.append("# --- Build models ---\n");
    py.append("def build_models() -> Dict[str, Any]:\n");
    py.append("    models: Dict[str, Any] = {}\n\n");

    Iterator<String> modelNames = modelsNode.fieldNames();
    while (modelNames.hasNext()) {
      String modelName = modelNames.next();
      JsonNode spec = modelsNode.get(modelName);

      String algorithm = requireText(spec, "algorithm");
      String typeRaw = requireText(spec, "type");
      ModelType type = ModelType.from(typeRaw);

      if (!ALGORITHMS.contains(algorithm)) {
        throw new IllegalArgumentException("algorithm no permitido: '" + algorithm + "'. Debe ser uno de: " + ALGORITHMS);
      }

      String ctor = resolveSklearnConstructor(algorithm, type);
      String kwargs = renderHyperparameters(spec.get("hyperparameters"));

      py.append("    # ").append(modelName).append("\n");
      py.append("    models['").append(escapePy(modelName)).append("'] = ")
          .append(ctor).append("(").append(kwargs).append(")\n\n");
    }
    py.append("    return models\n\n");

    py.append("# --- DSL model specifications ---\n");
    py.append("def model_specs() -> Dict[str, Dict[str, Any]]:\n");
    py.append("    specs: Dict[str, Dict[str, Any]] = {}\n\n");

    modelNames = modelsNode.fieldNames();
    while (modelNames.hasNext()) {
      String modelName = modelNames.next();
      JsonNode spec = modelsNode.get(modelName);

      String typeRaw = requireText(spec, "type");
      ModelType type = ModelType.from(typeRaw);
      String dataset = requireText(spec, "dataset");

      List<String> metrics = readMetrics(spec.path("training").path("metrics"));
      Schedule schedule = readSchedule(spec.path("training").path("schedule"));

      py.append("    specs['").append(escapePy(modelName)).append("'] = {\n");
      py.append("        'type': '").append(type.name()).append("',\n");
      py.append("        'dataset': r'").append(escapePy(dataset)).append("',\n");
      py.append("        'metrics': ").append(pythonList(metrics)).append(",\n");
      py.append("        'schedule': ").append(schedule.toPythonDict()).append("\n");
      py.append("    }\n\n");
    }
    py.append("    return specs\n\n");

    py.append("# --- Training ---\n");
    py.append("def train_one(model_name: str, model: Any, spec: Dict[str, Any], dataset_overrides: Optional[Dict[str, str]] = None) -> Tuple[Any, Dict[str, Any], List[str]]:\n");
    py.append("    dataset_path = resolve_dataset_for_model(model_name, spec, dataset_overrides)\n");
    py.append("    X, y, feature_cols = load_dataset(dataset_path, spec['type'])\n");
    py.append("    task = spec['type']\n");
    py.append("    split_cfg = dataset_specs().get(spec['dataset'], {'train_ratio': 0.7, 'val_ratio': 0.1, 'test_ratio': 0.2})\n");
    py.append("    train_ratio = split_cfg['train_ratio']\n");
    py.append("    val_ratio = split_cfg['val_ratio']\n");
    py.append("    test_ratio = split_cfg['test_ratio']\n");
    py.append("    total = train_ratio + val_ratio + test_ratio\n");
    py.append("    if abs(total - 1.0) > 1e-6:\n");
    py.append("        raise ValueError(f\"Ratios must sum to 1.0 for dataset {spec['dataset']}. Got {total}\")\n\n");

    py.append("    if task in ('Classification', 'Regression'):\n");
    py.append("        if y is None:\n");
    py.append("            raise ValueError(f\"Dataset '{dataset_path}' must provide y for {task}\")\n");
    py.append("        holdout_ratio = val_ratio + test_ratio\n");
    py.append("        if holdout_ratio <= 0 or train_ratio <= 0:\n");
    py.append("            raise ValueError('Invalid train/val/test ratios for supervised learning')\n");
    py.append("        X_train, X_holdout, y_train, y_holdout = train_test_split(\n");
    py.append("            X, y, test_size=holdout_ratio, random_state=42\n");
    py.append("        )\n");
    py.append("        if val_ratio > 0 and test_ratio > 0:\n");
    py.append("            test_fraction_within_holdout = test_ratio / (val_ratio + test_ratio)\n");
    py.append("            X_val, X_test, y_val, y_test = train_test_split(\n");
    py.append("                X_holdout, y_holdout, test_size=test_fraction_within_holdout, random_state=42\n");
    py.append("            )\n");
    py.append("        elif val_ratio > 0:\n");
    py.append("            X_val, y_val = X_holdout, y_holdout\n");
    py.append("            X_test, y_test = X_holdout.iloc[0:0], y_holdout.iloc[0:0]\n");
    py.append("        else:\n");
    py.append("            X_test, y_test = X_holdout, y_holdout\n");
    py.append("            X_val, y_val = X_holdout.iloc[0:0], y_holdout.iloc[0:0]\n");
    py.append("        model.fit(X_train, y_train)\n");
    py.append("        return model, {\n");
    py.append("            'train': (X_train, y_train),\n");
    py.append("            'val': (X_val, y_val),\n");
    py.append("            'test': (X_test, y_test)\n");
    py.append("        }, feature_cols\n\n");

    py.append("    elif task == 'Clustering':\n");
    py.append("        holdout_ratio = val_ratio + test_ratio\n");
    py.append("        if holdout_ratio > 0:\n");
    py.append("            X_train, X_holdout = train_test_split(X, test_size=holdout_ratio, random_state=42)\n");
    py.append("            if val_ratio > 0 and test_ratio > 0:\n");
    py.append("                test_fraction_within_holdout = test_ratio / (val_ratio + test_ratio)\n");
    py.append("                X_val, X_test = train_test_split(X_holdout, test_size=test_fraction_within_holdout, random_state=42)\n");
    py.append("            elif val_ratio > 0:\n");
    py.append("                X_val = X_holdout\n");
    py.append("                X_test = X_holdout.iloc[0:0]\n");
    py.append("            else:\n");
    py.append("                X_test = X_holdout\n");
    py.append("                X_val = X_holdout.iloc[0:0]\n");
    py.append("        else:\n");
    py.append("            X_train = X\n");
    py.append("            X_val = X.iloc[0:0]\n");
    py.append("            X_test = X.iloc[0:0]\n");
    py.append("        model.fit(X_train)\n");
    py.append("        return model, {\n");
    py.append("            'train': (X_train, None),\n");
    py.append("            'val': (X_val, None),\n");
    py.append("            'test': (X_test, None)\n");
    py.append("        }, feature_cols\n");
    py.append("    else:\n");
    py.append("        raise ValueError(f\"Unknown task type: {task}\")\n\n");

    py.append("# --- Evaluation ---\n");
    py.append("def evaluate_one(model_name: str, model: Any, spec: Dict[str, Any], split_data: Dict[str, Any]) -> Dict[str, Any]:\n");
    py.append("    if len(split_data['test'][0]) > 0:\n");
    py.append("        X_eval, y_eval = split_data['test']\n");
    py.append("    elif len(split_data['val'][0]) > 0:\n");
    py.append("        X_eval, y_eval = split_data['val']\n");
    py.append("    else:\n");
    py.append("        X_eval, y_eval = split_data['train']\n");
    py.append("    task = spec['type']\n");
    py.append("    metrics: List[str] = spec.get('metrics', [])\n");
    py.append("    results: Dict[str, Any] = {}\n\n");

    py.append("    if task == 'Classification':\n");
    py.append("        y_pred = model.predict(X_eval)\n");
    py.append("        for m in metrics:\n");
    py.append("            if m == 'accuracy':\n");
    py.append("                results[m] = float(accuracy_score(y_eval, y_pred))\n");
    py.append("            elif m == 'precision':\n");
    py.append("                results[m] = float(precision_score(y_eval, y_pred, average='weighted', zero_division=0))\n");
    py.append("            elif m == 'f1':\n");
    py.append("                results[m] = float(f1_score(y_eval, y_pred, average='weighted'))\n");
    py.append("            elif m == 'roc_auc':\n");
    py.append("                if hasattr(model, 'predict_proba'):\n");
    py.append("                    y_score = model.predict_proba(X_eval)\n");
    py.append("                elif hasattr(model, 'decision_function'):\n");
    py.append("                    y_score = model.decision_function(X_eval)\n");
    py.append("                else:\n");
    py.append("                    raise ValueError('roc_auc requires predict_proba or decision_function')\n");
    py.append("                y_score = np.asarray(y_score)\n");
    py.append("                if y_score.ndim == 2 and y_score.shape[1] == 2:\n");
    py.append("                    results[m] = float(roc_auc_score(y_eval, y_score[:, 1]))\n");
    py.append("                elif y_score.ndim == 2 and y_score.shape[1] > 2:\n");
    py.append("                    results[m] = float(roc_auc_score(y_eval, y_score, multi_class='ovo', average='weighted'))\n");
    py.append("                else:\n");
    py.append("                    results[m] = float(roc_auc_score(y_eval, y_score))\n");
    py.append("            elif m == 'confusion_matrix':\n");
    py.append("                results[m] = confusion_matrix(y_eval, y_pred).tolist()\n");
    py.append("            else:\n");
    py.append("                raise ValueError(f\"Unsupported classification metric: {m}\")\n");
    py.append("        return results\n\n");

    py.append("    if task == 'Regression':\n");
    py.append("        y_pred = model.predict(X_eval)\n");
    py.append("        for m in metrics:\n");
    py.append("            if m == 'mae':\n");
    py.append("                results[m] = float(mean_absolute_error(y_eval, y_pred))\n");
    py.append("            elif m == 'mse':\n");
    py.append("                results[m] = float(mean_squared_error(y_eval, y_pred))\n");
    py.append("            elif m == 'rmse':\n");
    py.append("                results[m] = float(np.sqrt(mean_squared_error(y_eval, y_pred)))\n");
    py.append("            else:\n");
    py.append("                raise ValueError(f\"Unsupported regression metric: {m}\")\n");
    py.append("        return results\n\n");

    py.append("    if task == 'Clustering':\n");
    py.append("        if hasattr(model, 'labels_'):\n");
    py.append("            labels = model.labels_\n");
    py.append("        elif hasattr(model, 'fit_predict'):\n");
    py.append("            labels = model.fit_predict(X_eval)\n");
    py.append("        elif hasattr(model, 'predict'):\n");
    py.append("            labels = model.predict(X_eval)\n");
    py.append("        else:\n");
    py.append("            raise ValueError('Clustering model does not expose labels_ nor fit_predict nor predict')\n\n");

    py.append("        for m in metrics:\n");
    py.append("            if m == 'silhouette':\n");
    py.append("                results[m] = float(silhouette_score(X_eval, labels))\n");
    py.append("            elif m == 'davies_bouldin':\n");
    py.append("                results[m] = float(davies_bouldin_score(X_eval, labels))\n");
    py.append("            elif m == 'calinski_harabasz':\n");
    py.append("                results[m] = float(calinski_harabasz_score(X_eval, labels))\n");
    py.append("            elif m == 'anomaly_rate':\n");
    py.append("                labels_arr = np.asarray(labels)\n");
    py.append("                results[m] = float(np.mean(labels_arr == -1))\n");
    py.append("            else:\n");
    py.append("                raise ValueError(f\"Unsupported clustering/anomaly metric: {m}\")\n");
    py.append("        return results\n\n");

    py.append("    raise ValueError(f\"Unknown task type: {task}\")\n\n");

    py.append("# --- Prediction ---\n");
    py.append("def predict_from_csv(model_name: str, in_csv: str, out_dir: str, out_csv: Optional[str] = None):\n");
    py.append("    model, feature_cols = load_artifact(model_name, out_dir)\n");
    py.append("    df = pd.read_csv(in_csv)\n");
    py.append("    if 'target' in df.columns:\n");
    py.append("        df = df.drop(columns=['target'])\n");
    py.append("    if 'label' in df.columns:\n");
    py.append("        df = df.drop(columns=['label'])\n");
    py.append("    X, _ = preprocess_X(df, feature_cols)\n");
    py.append("    y_pred = model.predict(X)\n");
    py.append("    if out_csv:\n");
    py.append("        pd.DataFrame({'prediction': y_pred}).to_csv(out_csv, index=False)\n");
    py.append("        print(f\"[predict] saved -> {out_csv}\")\n");
    py.append("    else:\n");
    py.append("        print(y_pred)\n\n");

    py.append("def predict_from_dataframe(model_name: str, df: pd.DataFrame, out_dir: str):\n");
    py.append("    model, feature_cols = load_artifact(model_name, out_dir)\n");
    py.append("    if 'target' in df.columns:\n");
    py.append("        df = df.drop(columns=['target'])\n");
    py.append("    if 'label' in df.columns:\n");
    py.append("        df = df.drop(columns=['label'])\n");
    py.append("    X, _ = preprocess_X(df, feature_cols)\n");
    py.append("    y_pred = model.predict(X)\n");
    py.append("    result = {'predictions': y_pred.tolist()}\n");
    py.append("    if hasattr(model, 'predict_proba'):\n");
    py.append("        try:\n");
    py.append("            result['probabilities'] = model.predict_proba(X).tolist()\n");
    py.append("        except Exception:\n");
    py.append("            pass\n");
    py.append("    return result\n\n");

    py.append("def predict_one_instance(model_name: str, instance: Dict[str, Any], out_dir: str):\n");
    py.append("    df = pd.DataFrame([instance])\n");
    py.append("    return predict_from_dataframe(model_name, df, out_dir)\n\n");

    py.append("# --- Retraining schedule ---\n");
    py.append("def parse_duration_seconds(expr: str) -> int:\n");
    py.append("    expr = str(expr).strip().lower()\n");
    py.append("    if expr.endswith('s'): return int(expr[:-1])\n");
    py.append("    if expr.endswith('m'): return int(expr[:-1]) * 60\n");
    py.append("    if expr.endswith('h'): return int(expr[:-1]) * 3600\n");
    py.append("    if expr.endswith('d'): return int(expr[:-1]) * 86400\n");
    py.append("    raise ValueError(f\"Unsupported duration format: {expr}. Use Ns/Nm/Nh/Nd\")\n\n");

    py.append("def setup_retraining(models: Dict[str, Any], specs: Dict[str, Dict[str, Any]], retrain_fn):\n");
    if (plan.needsScheduler) {
      py.append("    scheduler = BackgroundScheduler()\n");
    } else {
      py.append("    scheduler = None\n");
    }
    py.append("    for name, spec in specs.items():\n");
    py.append("        sch = spec.get('schedule', {})\n");
    py.append("        stype = sch.get('type')\n");
    py.append("        if stype == 'Periodic':\n");
    py.append("            every = sch.get('every')\n");
    py.append("            if not every:\n");
    py.append("                raise ValueError(f\"Periodic schedule requires 'every' for model {name}\")\n");
    py.append("            seconds = parse_duration_seconds(every)\n");
    if (plan.needsScheduler) {
      py.append("            scheduler.add_job(lambda n=name: retrain_fn(n), 'interval', seconds=seconds)\n");
    } else {
      py.append("            raise ValueError('Periodic schedule used but APScheduler not available')\n");
    }
    py.append("        elif stype == 'Event-Based':\n");
    py.append("            print(f\"[schedule] Event-Based for {name}: events={sch.get('events')}\")\n");
    py.append("        elif stype is None:\n");
    py.append("            pass\n");
    py.append("        else:\n");
    py.append("            raise ValueError(f\"Unknown schedule type: {stype}\")\n\n");
    if (plan.needsScheduler) {
      py.append("    if scheduler.get_jobs():\n");
      py.append("        scheduler.start()\n");
      py.append("        return scheduler\n");
      py.append("    return None\n\n");
    } else {
      py.append("    return None\n\n");
    }

    if (plan.needsRest) {
      py.append("# --- REST API ---\n");
      py.append("def create_rest_app(out_dir: str) -> FastAPI:\n");
      py.append("    app = FastAPI(title='Generated ML Model Service', version='1.0.0')\n\n");

      py.append("    @app.get('/health')\n");
      py.append("    def health():\n");
      py.append("        return {'status': 'ok'}\n\n");

      py.append("    @app.get('/models')\n");
      py.append("    def list_models():\n");
      py.append("        specs = model_specs()\n");
      py.append("        return {\n");
      py.append("            'models': [\n");
      py.append("                {\n");
      py.append("                    'name': name,\n");
      py.append("                    'type': spec['type'],\n");
      py.append("                    'dataset': spec['dataset'],\n");
      py.append("                    'metrics': spec.get('metrics', [])\n");
      py.append("                }\n");
      py.append("                for name, spec in specs.items()\n");
      py.append("            ]\n");
      py.append("        }\n\n");

      py.append("    @app.post('/predict/{model_name}')\n");
      py.append("    def predict(model_name: str, payload: Dict[str, Any]):\n");
      py.append("        try:\n");
      py.append("            instances = payload.get('instances')\n");
      py.append("            if not instances or not isinstance(instances, list):\n");
      py.append("                raise HTTPException(status_code=400, detail=\"Payload must contain a non-empty 'instances' list\")\n");
      py.append("            df = pd.DataFrame(instances)\n");
      py.append("            return predict_from_dataframe(model_name, df, out_dir)\n");
      py.append("        except FileNotFoundError:\n");
      py.append("            raise HTTPException(status_code=404, detail=f\"Artifact for model '{model_name}' not found\")\n");
      py.append("        except HTTPException:\n");
      py.append("            raise\n");
      py.append("        except Exception as e:\n");
      py.append("            raise HTTPException(status_code=500, detail=str(e))\n\n");

      py.append("    @app.post('/predict-batch/{model_name}')\n");
      py.append("    def predict_batch(model_name: str, payload: Dict[str, Any]):\n");
      py.append("        try:\n");
      py.append("            instances = payload.get('instances')\n");
      py.append("            if not instances or not isinstance(instances, list):\n");
      py.append("                raise HTTPException(status_code=400, detail=\"Payload must contain a non-empty 'instances' list\")\n");
      py.append("            df = pd.DataFrame(instances)\n");
      py.append("            return predict_from_dataframe(model_name, df, out_dir)\n");
      py.append("        except FileNotFoundError:\n");
      py.append("            raise HTTPException(status_code=404, detail=f\"Artifact for model '{model_name}' not found\")\n");
      py.append("        except HTTPException:\n");
      py.append("            raise\n");
      py.append("        except Exception as e:\n");
      py.append("            raise HTTPException(status_code=500, detail=str(e))\n\n");

      py.append("    @app.post('/reload/{model_name}')\n");
      py.append("    def reload_model(model_name: str):\n");
      py.append("        try:\n");
      py.append("            load_artifact(model_name, out_dir)\n");
      py.append("            return {'status': 'reloaded', 'model': model_name}\n");
      py.append("        except FileNotFoundError:\n");
      py.append("            raise HTTPException(status_code=404, detail=f\"Artifact for model '{model_name}' not found\")\n");
      py.append("        except Exception as e:\n");
      py.append("            raise HTTPException(status_code=500, detail=str(e))\n\n");

      py.append("    return app\n\n");
    }

    py.append("# --- CLI ---\n");
    py.append("def train_all(out_dir: str, dataset_overrides: Optional[Dict[str, str]] = None):\n");
    py.append("    models = build_models()\n");
    py.append("    specs = model_specs()\n");
    py.append("    for name in list(models.keys()):\n");
    py.append("        model, split_data, feature_cols = train_one(name, models[name], specs[name], dataset_overrides)\n");
    py.append("        models[name] = model\n");
    py.append("        if specs[name].get('metrics'):\n");
    py.append("            res = evaluate_one(name, model, specs[name], split_data)\n");
    py.append("            print(f\"[eval] {name} -> {res}\")\n");
    py.append("        path = save_artifact(name, model, feature_cols, out_dir)\n");
    py.append("        print(f\"[save] {name} -> {path}\")\n\n");

    py.append("def eval_all(out_dir: str, dataset_overrides: Optional[Dict[str, str]] = None):\n");
    py.append("    specs = model_specs()\n");
    py.append("    fresh_models = build_models()\n");
    py.append("    for name, spec in specs.items():\n");
    py.append("        if not spec.get('metrics'):\n");
    py.append("            continue\n");
    py.append("        model, _feature_cols = load_artifact(name, out_dir)\n");
    py.append("        _trained, split_data, _cols = train_one(name, fresh_models[name], spec, dataset_overrides)\n");
    py.append("        res = evaluate_one(name, model, spec, split_data)\n");
    py.append("        print(f\"[eval] {name} -> {res}\")\n\n");

    py.append("def serve_schedule(out_dir: str, dataset_overrides: Optional[Dict[str, str]] = None):\n");
    py.append("    models = build_models()\n");
    py.append("    specs = model_specs()\n\n");
    py.append("    for name in list(models.keys()):\n");
    py.append("        model, split_data, feature_cols = train_one(name, models[name], specs[name], dataset_overrides)\n");
    py.append("        models[name] = model\n");
    py.append("        save_artifact(name, model, feature_cols, out_dir)\n");
    py.append("        if specs[name].get('metrics'):\n");
    py.append("            print(f\"[eval] {name} -> {evaluate_one(name, model, specs[name], split_data)}\")\n\n");

    py.append("    def retrain_and_save(model_name: str):\n");
    py.append("        spec = specs[model_name]\n");
    py.append("        model = models[model_name]\n");
    py.append("        dataset_path = resolve_dataset_for_model(model_name, spec, dataset_overrides)\n");
    py.append("        print(f\"[retrain] {model_name} dataset={dataset_path}\")\n");
    py.append("        model, split_data, feature_cols = train_one(model_name, model, spec, dataset_overrides)\n");
    py.append("        models[model_name] = model\n");
    py.append("        save_artifact(model_name, model, feature_cols, out_dir)\n");
    py.append("        if spec.get('metrics'):\n");
    py.append("            print(f\"[eval] {model_name} -> {evaluate_one(model_name, model, spec, split_data)}\")\n\n");

    py.append("    sched = setup_retraining(models, specs, retrain_and_save)\n");
    py.append("    if sched is not None:\n");
    py.append("        print('[schedule] retraining scheduler started (Ctrl+C to stop)')\n");
    py.append("        try:\n");
    py.append("            while True:\n");
    py.append("                time.sleep(1)\n");
    py.append("        except KeyboardInterrupt:\n");
    py.append("            sched.shutdown()\n");
    py.append("    else:\n");
    py.append("        print('[schedule] no periodic jobs configured')\n\n");

    py.append("def main():\n");
    py.append("    parser = argparse.ArgumentParser()\n");
    py.append("    sub = parser.add_subparsers(dest='cmd', required=True)\n\n");

    py.append("    p_train = sub.add_parser('train')\n");
    py.append("    p_train.add_argument('--out-dir', default='artifacts')\n");
    py.append("    p_train.add_argument('--dataset', action='append', help=\"Override dataset path. Use either --dataset path.csv or --dataset model=path.csv\")\n\n");

    py.append("    p_eval = sub.add_parser('eval')\n");
    py.append("    p_eval.add_argument('--out-dir', default='artifacts')\n");
    py.append("    p_eval.add_argument('--dataset', action='append', help=\"Override dataset path. Use either --dataset path.csv or --dataset model=path.csv\")\n\n");

    py.append("    p_pred = sub.add_parser('predict')\n");
    py.append("    p_pred.add_argument('--model', required=True)\n");
    py.append("    p_pred.add_argument('--in-csv', required=True)\n");
    py.append("    p_pred.add_argument('--out-dir', default='artifacts')\n");
    py.append("    p_pred.add_argument('--out-csv')\n\n");

    py.append("    p_sched = sub.add_parser('serve-schedule')\n");
    py.append("    p_sched.add_argument('--out-dir', default='artifacts')\n");
    py.append("    p_sched.add_argument('--dataset', action='append', help=\"Override dataset path. Use either --dataset path.csv or --dataset model=path.csv\")\n\n");

    if (plan.needsRest) {
      py.append("    p_rest = sub.add_parser('serve-rest')\n");
      py.append("    p_rest.add_argument('--out-dir', default='artifacts')\n");
      py.append("    p_rest.add_argument('--host', default='127.0.0.1')\n");
      py.append("    p_rest.add_argument('--port', type=int, default=8000)\n\n");
    }

    py.append("    args = parser.parse_args()\n");
    py.append("    if args.cmd == 'train':\n");
    py.append("        train_all(args.out_dir, parse_dataset_overrides(args.dataset))\n");
    py.append("    elif args.cmd == 'eval':\n");
    py.append("        eval_all(args.out_dir, parse_dataset_overrides(args.dataset))\n");
    py.append("    elif args.cmd == 'predict':\n");
    py.append("        predict_from_csv(args.model, args.in_csv, args.out_dir, args.out_csv)\n");
    py.append("    elif args.cmd == 'serve-schedule':\n");
    py.append("        serve_schedule(args.out_dir, parse_dataset_overrides(args.dataset))\n");
    if (plan.needsRest) {
      py.append("    elif args.cmd == 'serve-rest':\n");
      py.append("        app = create_rest_app(args.out_dir)\n");
      py.append("        uvicorn.run(app, host=args.host, port=args.port)\n");
    }
    py.append("\n");
    py.append("if __name__ == '__main__':\n");
    py.append("    main()\n");

    return py.toString();
  }

  private static ImportPlan analyze(JsonNode modelsNode) {
    ImportPlan plan = new ImportPlan();

    Iterator<String> names = modelsNode.fieldNames();
    while (names.hasNext()) {
      String modelName = names.next();
      JsonNode spec = modelsNode.get(modelName);

      String algorithm = requireText(spec, "algorithm");
      String typeRaw = requireText(spec, "type");
      ModelType type = ModelType.from(typeRaw);

      if (!ALGORITHMS.contains(algorithm)) {
        throw new IllegalArgumentException("algorithm no permitido: '" + algorithm + "' en modelo " + modelName);
      }

      plan.estimatorImports.addAll(estimatorImportsFor(algorithm));

      JsonNode stype = spec.path("training").path("schedule").path("type");
      if (stype.isTextual() && "Periodic".equalsIgnoreCase(stype.asText())) {
        plan.needsScheduler = true;
      }

      List<String> metrics = readMetrics(spec.path("training").path("metrics"));
      for (String m : metrics) {
        if (!METRICS.contains(m)) {
          throw new IllegalArgumentException("Métrica fuera del enum del DSL: " + m + " (modelo " + modelName + ")");
        }
        ensureMetricCompatible(type, m, modelName);
        plan.metricImports.addAll(metricImportsFor(m));
      }

      resolveSklearnConstructor(algorithm, type);
    }

    plan.needsRest = true;
    return plan;
  }

  private static DatasetSplitSpec readDatasetSplitSpec(String datasetName, JsonNode datasetsNode) {
    JsonNode ds = datasetsNode.get(datasetName);
    if (ds == null || ds.isNull() || ds.isMissingNode()) {
      throw new IllegalArgumentException("El modelo referencia un dataset no definido: '" + datasetName + "'");
    }

    double train = readRatio(ds, "train-ratio", 0.7);
    double val = readRatio(ds, "val-ratio", 0.1);
    double test = readRatio(ds, "test-ratio", 0.2);

    double sum = train + val + test;
    if (Math.abs(sum - 1.0) > 1e-6) {
      throw new IllegalArgumentException(
          "Los ratios del dataset '" + datasetName + "' deben sumar 1.0. Valor actual: " + sum);
    }

    return new DatasetSplitSpec(train, val, test);
  }

  private static double readRatio(JsonNode ds, String field, double defaultValue) {
    JsonNode n = ds.get(field);
    if (n == null || n.isNull() || n.isMissingNode()) {
      return defaultValue;
    }
    return n.asDouble();
  }

  private static String pythonFloat(double d) {
    if (d == (long) d) {
      return String.format(Locale.ROOT, "%d.0", (long) d);
    }
    return String.format(Locale.ROOT, "%s", d);
  }

  private static List<Map<String, String>> readEvents(JsonNode eventsNode) {
    if (eventsNode == null || eventsNode.isMissingNode() || eventsNode.isNull()) {
      return List.of();
    }

    if (!eventsNode.isArray()) {
      throw new IllegalArgumentException("'events' debe ser un array");
    }

    List<Map<String, String>> events = new ArrayList<>();
    for (JsonNode ev : eventsNode) {
      String elementId = optionalText(ev, "element-id");
      String event = optionalText(ev, "event");

      Map<String, String> item = new LinkedHashMap<>();
      if (elementId != null) item.put("element-id", elementId);
      if (event != null) item.put("event", event);

      events.add(item);
    }
    return events;
  }

  private static void ensureMetricCompatible(ModelType type, String metric, String modelName) {
    switch (type) {
      case Classification:
        if (Set.of("mae", "mse", "rmse", "silhouette", "davies_bouldin", "calinski_harabasz", "anomaly_rate").contains(metric)) {
          throw new IllegalArgumentException("Métrica '" + metric + "' no compatible con Classification (modelo " + modelName + ")");
        }
        break;
      case Regression:
        if (Set.of("accuracy", "precision", "f1", "roc_auc", "confusion_matrix", "silhouette", "davies_bouldin", "calinski_harabasz", "anomaly_rate").contains(metric)) {
          throw new IllegalArgumentException("Métrica '" + metric + "' no compatible con Regression (modelo " + modelName + ")");
        }
        break;
      case Clustering:
        if (Set.of("accuracy", "precision", "f1", "roc_auc", "confusion_matrix", "mae", "mse", "rmse").contains(metric)) {
          throw new IllegalArgumentException("Métrica '" + metric + "' no compatible con Clustering (modelo " + modelName + ")");
        }
        break;
    }
  }

  private static List<String> metricImportsFor(String metric) {
    switch (metric) {
      case "accuracy":
        return List.of("accuracy_score");
      case "precision":
        return List.of("precision_score");
      case "f1":
        return List.of("f1_score");
      case "roc_auc":
        return List.of("roc_auc_score");
      case "confusion_matrix":
        return List.of("confusion_matrix");
      case "mae":
        return List.of("mean_absolute_error");
      case "mse":
      case "rmse":
        return List.of("mean_squared_error");
      case "silhouette":
        return List.of("silhouette_score");
      case "davies_bouldin":
        return List.of("davies_bouldin_score");
      case "calinski_harabasz":
        return List.of("calinski_harabasz_score");
      case "anomaly_rate":
        return List.of();
      default:
        throw new IllegalArgumentException("Metric no soportada: " + metric);
    }
  }

  private static List<String> estimatorImportsFor(String algorithm) {
    switch (algorithm) {
      case "Logistic Regression":
        return List.of("from sklearn.linear_model import LogisticRegression");
      case "Decision Trees":
        return List.of("from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor");
      case "Random Forest":
        return List.of("from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor");
      case "SVM":
        return List.of("from sklearn.svm import SVC, SVR");
      case "Neural Networks":
        return List.of("from sklearn.neural_network import MLPClassifier, MLPRegressor");
      case "K-Means":
        return List.of("from sklearn.cluster import KMeans");
      case "Hierarchical Clustering":
        return List.of("from sklearn.cluster import AgglomerativeClustering");
      case "DBSCAN":
        return List.of("from sklearn.cluster import DBSCAN");
      default:
        throw new IllegalArgumentException("Algorithm no soportado: " + algorithm);
    }
  }

  private static String resolveSklearnConstructor(String algorithm, ModelType type) {
    switch (algorithm) {
      case "Logistic Regression":
        if (type != ModelType.Classification) {
          throw new IllegalArgumentException("Logistic Regression solo es válido para type=Classification.");
        }
        return "LogisticRegression";

      case "Decision Trees":
        if (type == ModelType.Clustering) {
          throw new IllegalArgumentException("Decision Trees no encaja con type=Clustering.");
        }
        return (type == ModelType.Regression) ? "DecisionTreeRegressor" : "DecisionTreeClassifier";

      case "Random Forest":
        if (type == ModelType.Clustering) {
          throw new IllegalArgumentException("Random Forest no encaja con type=Clustering.");
        }
        return (type == ModelType.Regression) ? "RandomForestRegressor" : "RandomForestClassifier";

      case "SVM":
        if (type == ModelType.Clustering) {
          throw new IllegalArgumentException("SVM no encaja con type=Clustering.");
        }
        return (type == ModelType.Regression) ? "SVR" : "SVC";

      case "Neural Networks":
        if (type == ModelType.Clustering) {
          throw new IllegalArgumentException("Neural Networks no encaja con type=Clustering.");
        }
        return (type == ModelType.Regression) ? "MLPRegressor" : "MLPClassifier";

      case "K-Means":
        if (type != ModelType.Clustering) {
          throw new IllegalArgumentException("K-Means requiere type=Clustering.");
        }
        return "KMeans";

      case "Hierarchical Clustering":
        if (type != ModelType.Clustering) {
          throw new IllegalArgumentException("Hierarchical Clustering requiere type=Clustering.");
        }
        return "AgglomerativeClustering";

      case "DBSCAN":
        if (type != ModelType.Clustering) {
          throw new IllegalArgumentException("DBSCAN requiere type=Clustering.");
        }
        return "DBSCAN";

      default:
        throw new IllegalArgumentException("Algorithm no soportado: " + algorithm);
    }
  }

  private static List<String> readMetrics(JsonNode metricsNode) {
    if (metricsNode == null || metricsNode.isMissingNode() || metricsNode.isNull()) {
      return List.of();
    }
    if (!metricsNode.isArray()) {
      throw new IllegalArgumentException("'training.metrics' debe ser un array de strings");
    }
    List<String> res = new ArrayList<>();
    for (JsonNode m : metricsNode) {
      if (!m.isTextual()) {
        throw new IllegalArgumentException("'training.metrics' debe contener strings");
      }
      res.add(m.asText());
    }
    return res;
  }

  private static Schedule readSchedule(JsonNode scheduleNode) {
    if (scheduleNode == null || scheduleNode.isMissingNode() || scheduleNode.isNull()) {
      return Schedule.none();
    }

    String type = optionalText(scheduleNode, "type");
    if (type == null) {
      return Schedule.none();
    }

    if ("Periodic".equalsIgnoreCase(type)) {
      String every = optionalText(scheduleNode, "every");
      return Schedule.periodic(every);
    }

    if ("Event-Based".equalsIgnoreCase(type) || "EventBased".equalsIgnoreCase(type)) {
      JsonNode eventsNode = scheduleNode.get("events");
      return Schedule.eventBased(readEvents(eventsNode));
    }

    throw new IllegalArgumentException("schedule.type desconocido: " + type);
  }

  private static String renderHyperparameters(JsonNode hp) {
    if (hp == null || hp.isNull() || hp.isMissingNode()) {
      return "";
    }
    if (!hp.isObject()) {
      throw new IllegalArgumentException("'hyperparameters' debe ser un objeto (map param -> value).");
    }
    List<String> parts = new ArrayList<>();
    hp.fields().forEachRemaining(e -> parts.add(e.getKey() + "=" + pythonLiteral(e.getValue())));
    return String.join(", ", parts);
  }

  private static String pythonLiteral(JsonNode v) {
    if (v.isNumber()) {
      return v.numberValue().toString();
    }
    if (v.isBoolean()) {
      return v.booleanValue() ? "True" : "False";
    }
    String s = v.asText().replace("\\", "\\\\").replace("'", "\\'");
    return "'" + s + "'";
  }

  private static String pythonList(List<String> items) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append("'").append(items.get(i).replace("\\", "\\\\").replace("'", "\\'")).append("'");
    }
    sb.append("]");
    return sb.toString();
  }

  private static String escapePy(String s) {
    return s.replace("\\", "\\\\").replace("'", "\\'");
  }

  private static String requireText(JsonNode obj, String field) {
    JsonNode n = obj.get(field);
    if (n == null || n.isNull() || n.isMissingNode()) {
      throw new IllegalArgumentException("Falta campo obligatorio: '" + field + "'");
    }
    return n.asText();
  }

  private static String optionalText(JsonNode obj, String field) {
    JsonNode n = obj.get(field);
    if (n == null || n.isNull() || n.isMissingNode()) {
      return null;
    }
    return n.asText();
  }

  private static class ImportPlan {
    final LinkedHashSet<String> estimatorImports = new LinkedHashSet<>();
    final LinkedHashSet<String> metricImports = new LinkedHashSet<>();
    boolean needsScheduler = false;
    boolean needsRest = false;
  }

  private static class DatasetSplitSpec {
    final double train;
    final double val;
    final double test;

    DatasetSplitSpec(double train, double val, double test) {
      this.train = train;
      this.val = val;
      this.test = test;
    }
  }

  private static class Schedule {
    final String type;
    final String every;
    final List<Map<String, String>> events;

    private Schedule(String type, String every, List<Map<String, String>> events) {
      this.type = type;
      this.every = every;
      this.events = events;
    }

    static Schedule none() {
      return new Schedule(null, null, List.of());
    }

    static Schedule periodic(String every) {
      return new Schedule("Periodic", every, List.of());
    }

    static Schedule eventBased(List<Map<String, String>> events) {
      return new Schedule("Event-Based", null, events);
    }

    String toPythonDict() {
      if (type == null) {
        return "{}";
      }
      if ("Periodic".equalsIgnoreCase(type)) {
        return "{'type': 'Periodic', 'every': " + (every == null ? "None" : ("'" + esc(every) + "'")) + "}";
      }
      return "{'type': 'Event-Based', 'events': " + eventsToPython(events) + "}";
    }

    private static String eventsToPython(List<Map<String, String>> events) {
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < events.size(); i++) {
        if (i > 0) sb.append(", ");
        Map<String, String> ev = events.get(i);
        sb.append("{");
        int j = 0;
        for (Map.Entry<String, String> e : ev.entrySet()) {
          if (j > 0) sb.append(", ");
          sb.append("'").append(esc(e.getKey())).append("': ");
          sb.append("'").append(esc(e.getValue())).append("'");
          j++;
        }
        sb.append("}");
      }
      sb.append("]");
      return sb.toString();
    }

    private static String esc(String s) {
      return s.replace("\\", "\\\\").replace("'", "\\'");
    }
  }
}