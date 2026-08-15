#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesRegressor, IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
)
from sklearn.model_selection import LeaveOneGroupOut, LeaveOneOut
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from sparkevitune.features import ANOMALY_FEATURE_COLUMNS, PREDICTION_FEATURE_COLUMNS
from sparkevitune.models import ClusterProfile
from sparkevitune.validator import ConstraintValidator


def regression_model(seed: int = 42) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            (
                "model",
                ExtraTreesRegressor(
                    n_estimators=500,
                    min_samples_leaf=1,
                    random_state=seed,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def regression_metrics(y: np.ndarray, pred: np.ndarray) -> dict[str, float]:
    errors = np.abs(y - pred)
    return {
        "n": len(y),
        "mae_s": float(mean_absolute_error(y, pred)),
        "rmse_s": float(math.sqrt(mean_squared_error(y, pred))),
        "mape_pct": float(np.mean(errors / np.maximum(np.abs(y), 1e-9)) * 100.0),
        "r2": float(r2_score(y, pred)) if len(y) > 1 else float("nan"),
    }


def cross_validated_predictions(frame: pd.DataFrame, splitter, groups=None) -> np.ndarray:
    X = frame[PREDICTION_FEATURE_COLUMNS]
    y = frame["target_duration_s"].to_numpy(float)
    pred = np.zeros(len(frame), dtype=float)
    iterator = splitter.split(X, y, groups) if groups is not None else splitter.split(X, y)
    for train_idx, test_idx in iterator:
        model = regression_model()
        model.fit(X.iloc[train_idx], y[train_idx])
        pred[test_idx] = model.predict(X.iloc[test_idx])
    return pred


def rule_config_anomaly(row: pd.Series) -> int:
    workload = str(row["workload"])
    return int(
        row["aqe_enabled"] < 0.5
        or row["kryo_enabled"] < 0.5
        or (workload == "skew_join" and row["skew_join_enabled"] < 0.5)
        or (workload in {"sql_joins", "heavy_shuffle"} and row["coalesce_enabled"] < 0.5)
        or row["shuffle_partitions"] >= 400
    )


def independent_safe(config: dict[str, Any], cluster: ClusterProfile) -> bool:
    try:
        mem = str(config.get("spark.executor.memory", "1g")).lower()
        if mem.endswith("g"):
            heap = float(mem[:-1])
        elif mem.endswith("m"):
            heap = float(mem[:-1]) / 1024.0
        else:
            return False
        overhead = max(0.384, heap * 0.10)
        cores = int(config.get("spark.executor.cores", 1))
        instances = int(config.get("spark.executor.instances", cluster.workers))
        partitions = int(config.get("spark.sql.shuffle.partitions", 200))
        fraction = float(config.get("spark.memory.fraction", 0.6))
    except (TypeError, ValueError):
        return False
    if heap + overhead > cluster.memory_per_worker_gb * 0.90:
        return False
    if cores < 1 or cores > cluster.cores_per_worker:
        return False
    if instances < 1 or instances * cores > cluster.total_cores:
        return False
    if instances * (heap + overhead) > cluster.total_memory_gb * 0.90:
        return False
    if not 2 <= partitions <= 20000 or not 0.4 <= fraction <= 0.8:
        return False
    for key in config:
        lowered = key.lower()
        if any(token in lowered for token in ("password", "secret", "token", "api.key", "access.key")):
            return False
    for key in ("spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled"):
        if key in config and str(config[key]).lower() not in {"true", "false"}:
            return False
    return True


def validator_study(seed: int = 42, per_category: int = 100) -> tuple[pd.DataFrame, dict[str, float]]:
    rng = np.random.default_rng(seed)
    cluster = ClusterProfile(workers=2, cores_per_worker=4, memory_per_worker_gb=8)
    validator = ConstraintValidator()
    categories = [
        "safe",
        "memory_overcommit",
        "cpu_overcommit",
        "instances_overcommit",
        "invalid_memory",
        "invalid_cores",
        "partitions_out_of_range",
        "forbidden_secret",
        "unsupported_parameter",
        "invalid_boolean",
        "memory_fraction_out_of_range",
    ]
    rows: list[dict[str, Any]] = []
    for category in categories:
        for _ in range(per_category):
            candidate: dict[str, Any] = {
                "spark.executor.memory": "2g",
                "spark.executor.cores": 2,
                "spark.executor.instances": 2,
                "spark.sql.shuffle.partitions": int(rng.choice([50, 100, 200])),
                "spark.memory.fraction": 0.6,
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
            }
            unsafe = category != "safe"
            if category == "memory_overcommit":
                candidate["spark.executor.memory"] = "16g"
            elif category == "cpu_overcommit":
                candidate["spark.executor.cores"] = 16
            elif category == "instances_overcommit":
                candidate["spark.executor.instances"] = 20
            elif category == "invalid_memory":
                candidate["spark.executor.memory"] = "many"
            elif category == "invalid_cores":
                candidate["spark.executor.cores"] = "many"
            elif category == "partitions_out_of_range":
                candidate["spark.sql.shuffle.partitions"] = int(rng.choice([0, 50000]))
            elif category == "forbidden_secret":
                candidate["spark.authenticate.secret"] = "do-not-store"
            elif category == "unsupported_parameter":
                candidate["spark.evil.command"] = "rm -rf /"
            elif category == "invalid_boolean":
                candidate["spark.sql.adaptive.enabled"] = "yes"
            elif category == "memory_fraction_out_of_range":
                candidate["spark.memory.fraction"] = float(rng.choice([0.1, 1.5]))

            result = validator.validate({}, candidate, cluster)
            detected = bool(result.violations or result.adjustments)
            final_safe = independent_safe(result.configuration, cluster)
            accepted = result.valid
            rows.append(
                {
                    "category": category,
                    "unsafe": int(unsafe),
                    "detected": int(detected),
                    "valid": int(accepted),
                    "final_safe": int(final_safe),
                    "adjusted": int(bool(result.adjustments)),
                    "violations": len(result.violations),
                    "unsafe_accepted": int(unsafe and accepted and not final_safe),
                    "safe_rejected": int((not unsafe) and not accepted),
                }
            )
    data = pd.DataFrame(rows)
    unsafe = data[data["unsafe"] == 1]
    safe = data[data["unsafe"] == 0]
    metrics = {
        "candidates": len(data),
        "unsafe_detection_rate_pct": float(unsafe["detected"].mean() * 100),
        "unsafe_final_containment_rate_pct": float((~((unsafe["valid"] == 1) & (unsafe["final_safe"] == 0))).mean() * 100),
        "unsafe_accepted_count": int(unsafe["unsafe_accepted"].sum()),
        "false_rejection_rate_pct": float(safe["safe_rejected"].mean() * 100),
        "safe_unmodified_rate_pct": float((safe["detected"] == 0).mean() * 100),
    }
    return data, metrics


def save_plots(frame: pd.DataFrame, out: Path, loo_pred: np.ndarray, logo_pred: np.ndarray, anomaly_pred: np.ndarray, anomaly_score: np.ndarray, validator_df: pd.DataFrame) -> None:
    figures = out / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(6.4, 5.0))
    plt.scatter(frame["target_duration_s"], loo_pred)
    lo = min(frame["target_duration_s"].min(), loo_pred.min())
    hi = max(frame["target_duration_s"].max(), loo_pred.max())
    plt.plot([lo, hi], [lo, hi], linestyle="--")
    for _, row in frame.iterrows():
        i = frame.index.get_loc(row.name)
        plt.annotate(f"{row['workload']}-{row['variant'][0]}", (row["target_duration_s"], loo_pred[i]), fontsize=7)
    plt.xlabel("Observed runtime (s)")
    plt.ylabel("LOO predicted runtime (s)")
    plt.title("RQ1 pilot: predicted vs observed runtime")
    plt.tight_layout()
    plt.savefig(figures / "rq1_predicted_vs_observed.png", dpi=220)
    plt.close()

    errors = frame.assign(abs_error=np.abs(frame["target_duration_s"].to_numpy() - loo_pred)).groupby("workload", as_index=False)["abs_error"].mean()
    plt.figure(figsize=(6.4, 4.2))
    plt.bar(errors["workload"], errors["abs_error"])
    plt.ylabel("MAE (s)")
    plt.title("RQ1 pilot: runtime error by workload")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(figures / "rq1_error_by_workload.png", dpi=220)
    plt.close()

    anomaly_metrics = pd.DataFrame(
        {
            "metric": ["Precision", "Recall", "F1", "Average precision"],
            "value": [
                precision_score(frame["label_anomaly"], anomaly_pred, zero_division=0),
                recall_score(frame["label_anomaly"], anomaly_pred, zero_division=0),
                f1_score(frame["label_anomaly"], anomaly_pred, zero_division=0),
                average_precision_score(frame["label_anomaly"], anomaly_score),
            ],
        }
    )
    plt.figure(figsize=(6.4, 4.2))
    plt.bar(anomaly_metrics["metric"], anomaly_metrics["value"])
    plt.ylim(0, 1.05)
    plt.ylabel("Score")
    plt.title("RQ2 pilot: Isolation Forest performance")
    plt.xticks(rotation=18)
    plt.tight_layout()
    plt.savefig(figures / "rq2_anomaly_metrics.png", dpi=220)
    plt.close()

    pairs = frame.pivot(index="workload", columns="variant", values="duration_s").reset_index()
    x = np.arange(len(pairs))
    width = 0.36
    plt.figure(figsize=(7.2, 4.5))
    plt.bar(x - width / 2, pairs["bad"], width, label="Bad/baseline")
    plt.bar(x + width / 2, pairs["optimized"], width, label="Optimized")
    plt.xticks(x, pairs["workload"], rotation=18)
    plt.ylabel("Runtime (s)")
    plt.title("RQ3 available real evidence: before/after runtime")
    plt.legend()
    plt.tight_layout()
    plt.savefig(figures / "rq3_runtime_before_after.png", dpi=220)
    plt.close()

    logo_errors = frame.assign(abs_error=np.abs(frame["target_duration_s"].to_numpy() - logo_pred)).groupby("workload", as_index=False)["abs_error"].mean()
    plt.figure(figsize=(6.4, 4.2))
    plt.bar(logo_errors["workload"], logo_errors["abs_error"])
    plt.ylabel("Held-out workload MAE (s)")
    plt.title("RQ4 pilot: leave-one-workload-out generalization")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(figures / "rq4_generalization.png", dpi=220)
    plt.close()

    summary = validator_df.groupby("category", as_index=False).agg(detected=("detected", "sum"), accepted=("valid", "sum"), final_safe=("final_safe", "sum"))
    summary = summary.sort_values("category")
    plt.figure(figsize=(8.0, 5.5))
    plt.barh(summary["category"], summary["detected"])
    plt.xlabel("Detected candidates")
    plt.title("RQ5: validator detections by generated category")
    plt.tight_layout()
    plt.savefig(figures / "rq5_validator_impact.png", dpi=220)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Quantitative pilot evaluation for RQ1-RQ5.")
    parser.add_argument("--csv", default="data/real/historical_real_runs.csv")
    parser.add_argument("--out", default="benchmarks/results/pilot")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(args.csv).reset_index(drop=True)

    loo_pred = cross_validated_predictions(frame, LeaveOneOut())
    loo_metrics = regression_metrics(frame["target_duration_s"].to_numpy(float), loo_pred)
    baseline_pred = np.array([
        frame.drop(index=i)["target_duration_s"].median() for i in range(len(frame))
    ])
    baseline_metrics = regression_metrics(frame["target_duration_s"].to_numpy(float), baseline_pred)

    groups = frame["workload"].to_numpy()
    logo_pred = cross_validated_predictions(frame, LeaveOneGroupOut(), groups=groups)
    logo_metrics = regression_metrics(frame["target_duration_s"].to_numpy(float), logo_pred)
    logo_by_workload = []
    for workload, subset in frame.assign(pred=logo_pred).groupby("workload"):
        logo_by_workload.append({"workload": workload, **regression_metrics(subset["target_duration_s"].to_numpy(), subset["pred"].to_numpy())})

    normal = frame[frame["label_anomaly"] == 0]
    anomaly_model = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", RobustScaler()),
            ("model", IsolationForest(n_estimators=500, contamination="auto", random_state=42, n_jobs=-1)),
        ]
    )
    anomaly_model.fit(normal[ANOMALY_FEATURE_COLUMNS])
    raw_pred = anomaly_model.predict(frame[ANOMALY_FEATURE_COLUMNS])
    anomaly_pred = (raw_pred == -1).astype(int)
    anomaly_score = -anomaly_model.decision_function(frame[ANOMALY_FEATURE_COLUMNS])
    y_anomaly = frame["label_anomaly"].to_numpy(int)
    anomaly_metrics = {
        "n": len(frame),
        "normal_training_rows": len(normal),
        "precision": float(precision_score(y_anomaly, anomaly_pred, zero_division=0)),
        "recall": float(recall_score(y_anomaly, anomaly_pred, zero_division=0)),
        "f1": float(f1_score(y_anomaly, anomaly_pred, zero_division=0)),
        "average_precision": float(average_precision_score(y_anomaly, anomaly_score)),
    }
    rules_pred = frame.apply(rule_config_anomaly, axis=1).to_numpy(int)
    rules_metrics = {
        "precision": float(precision_score(y_anomaly, rules_pred, zero_division=0)),
        "recall": float(recall_score(y_anomaly, rules_pred, zero_division=0)),
        "f1": float(f1_score(y_anomaly, rules_pred, zero_division=0)),
    }
    combined = np.maximum(anomaly_pred, rules_pred)
    combined_metrics = {
        "precision": float(precision_score(y_anomaly, combined, zero_division=0)),
        "recall": float(recall_score(y_anomaly, combined, zero_division=0)),
        "f1": float(f1_score(y_anomaly, combined, zero_division=0)),
    }

    pairs = frame.pivot(index="workload", columns="variant", values="duration_s")
    pairs["speedup"] = pairs["bad"] / pairs["optimized"]
    pairs["change_pct"] = (pairs["optimized"] - pairs["bad"]) / pairs["bad"] * 100.0
    rq3 = {
        "workloads": len(pairs),
        "improved_workloads": int((pairs["speedup"] > 1).sum()),
        "regressed_workloads": int((pairs["speedup"] < 1).sum()),
        "median_speedup": float(pairs["speedup"].median()),
        "mean_speedup": float(pairs["speedup"].mean()),
        "geometric_mean_speedup": float(np.exp(np.log(pairs["speedup"]).mean())),
        "note": "These are real before/after rule-based runs; they do not validate Bayesian optimization.",
    }

    validator_df, validator_metrics = validator_study()

    results = {
        "corpus": {
            "rows": len(frame),
            "workloads": sorted(frame["workload"].unique().tolist()),
            "clusters": sorted(frame["cluster"].unique().tolist()),
            "repetitions_per_configuration": 1,
            "raw_event_logs_available": False,
            "scope": "historical real-run pilot; not the required repeated multi-cluster benchmark",
        },
        "RQ1_runtime_prediction": {
            "extra_trees_leave_one_run_out": loo_metrics,
            "median_baseline_leave_one_run_out": baseline_metrics,
            "model_beats_baseline_mae": bool(loo_metrics["mae_s"] < baseline_metrics["mae_s"]),
        },
        "RQ2_anomaly_detection": {
            "isolation_forest": anomaly_metrics,
            "deterministic_config_rules": rules_metrics,
            "rules_plus_isolation_forest": combined_metrics,
            "label_definition": "deliberately bad configuration versus optimized configuration",
        },
        "RQ3_optimization": {**rq3, "per_workload": pairs.reset_index().to_dict(orient="records")},
        "RQ4_generalization": {
            "leave_one_workload_out": logo_metrics,
            "per_held_out_workload": logo_by_workload,
            "conclusion": "pilot only; one cluster and one data size cannot establish robustness",
        },
        "RQ5_validator": validator_metrics,
        "submission_readiness": {
            "RQ1": "pilot evidence only",
            "RQ2": "pilot evidence only",
            "RQ3": "rules-only before/after evidence; Bayesian optimization unvalidated",
            "RQ4": "not validated",
            "RQ5": "quantified generated-candidate study; real optimizer candidate study still required",
        },
    }

    (out / "rq_results.json").write_text(json.dumps(results, indent=2, allow_nan=True), encoding="utf-8")
    predictions = frame[["run_id", "workload", "variant", "target_duration_s", "label_anomaly"]].copy()
    predictions["loo_prediction_s"] = loo_pred
    predictions["logo_prediction_s"] = logo_pred
    predictions["isolation_forest_prediction"] = anomaly_pred
    predictions["isolation_forest_score"] = anomaly_score
    predictions["rule_prediction"] = rules_pred
    predictions.to_csv(out / "pilot_predictions.csv", index=False)
    validator_df.to_csv(out / "validator_study.csv", index=False)
    pairs.reset_index().to_csv(out / "runtime_before_after.csv", index=False)
    save_plots(frame, out, loo_pred, logo_pred, anomaly_pred, anomaly_score, validator_df)

    md = "# SparkEviTune RQ1-RQ5 quantitative pilot\n\n"
    md += "This report uses **eight historical real Spark runs** recovered from preserved notebook outputs. " \
          "It is not the repeated multi-cluster benchmark required for submission.\n\n"
    md += "## RQ1 — Runtime prediction\n\n"
    md += f"Extra Trees LOO: MAE **{loo_metrics['mae_s']:.2f} s**, RMSE **{loo_metrics['rmse_s']:.2f} s**, MAPE **{loo_metrics['mape_pct']:.1f}%**, R² **{loo_metrics['r2']:.3f}**.  \n"
    md += f"Median baseline LOO: MAE **{baseline_metrics['mae_s']:.2f} s**. The model {'beats' if loo_metrics['mae_s'] < baseline_metrics['mae_s'] else 'does not beat'} this baseline.\n\n"
    md += "## RQ2 — Anomaly detection\n\n"
    md += f"Isolation Forest: precision **{anomaly_metrics['precision']:.3f}**, recall **{anomaly_metrics['recall']:.3f}**, F1 **{anomaly_metrics['f1']:.3f}**, AP **{anomaly_metrics['average_precision']:.3f}**.  \n"
    md += f"Deterministic configuration rules: F1 **{rules_metrics['f1']:.3f}**. Labels denote deliberately bad versus optimized configurations, so this comparison is not an unknown-anomaly study.\n\n"
    md += "## RQ3 — Optimization\n\n"
    md += f"Real before/after runs improved **{rq3['improved_workloads']}/{rq3['workloads']}** workloads; median speedup **{rq3['median_speedup']:.3f}×**, geometric mean **{rq3['geometric_mean_speedup']:.3f}×**. Bayesian optimization remains unvalidated because its candidates were not executed.\n\n"
    md += "## RQ4 — Generalization\n\n"
    md += f"Leave-one-workload-out MAE **{logo_metrics['mae_s']:.2f} s**, MAPE **{logo_metrics['mape_pct']:.1f}%**, R² **{logo_metrics['r2']:.3f}**. One cluster and one data size cannot establish cross-cluster robustness.\n\n"
    md += "## RQ5 — Constraint validator\n\n"
    md += f"Generated candidates: **{validator_metrics['candidates']}**; unsafe detection **{validator_metrics['unsafe_detection_rate_pct']:.1f}%**; unsafe accepted **{validator_metrics['unsafe_accepted_count']}**; false rejection **{validator_metrics['false_rejection_rate_pct']:.1f}%**. This is a deterministic generated-candidate study, not a deployed-cluster safety proof.\n\n"
    md += "## Submission decision\n\nThe implementation is executable and the historical pilot is quantified, but the real ≥5-repetition, multi-cluster experiment remains mandatory before presenting RQ1–RQ4 as validated findings.\n"
    (out / "PILOT_RQ_REPORT.md").write_text(md, encoding="utf-8")
    print(json.dumps(results, indent=2, allow_nan=True))


if __name__ == "__main__":
    main()
