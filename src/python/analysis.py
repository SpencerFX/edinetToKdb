#!/usr/bin/env python3

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix
)


def safe_divide(a, b):
    """Elementwise safe division."""
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    out = a / b.replace({0: np.nan})
    return out.replace([np.inf, -np.inf], np.nan)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build default-risk features from annual statement data.
    Expected columns:
      revenue, operating_income, net_income, assets, liabilities, equity, operating_cash_flow
    """
    out = df.copy()

    numeric_cols = [
        "revenue",
        "operating_income",
        "net_income",
        "assets",
        "liabilities",
        "equity",
        "operating_cash_flow",
    ]
    for c in numeric_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["roe"] = safe_divide(out["net_income"], out["equity"])
    out["debt_ratio"] = safe_divide(out["liabilities"], out["assets"])
    out["equity_ratio"] = safe_divide(out["equity"], out["assets"])
    out["operating_margin"] = safe_divide(out["operating_income"], out["revenue"])
    out["net_income_margin"] = safe_divide(out["net_income"], out["revenue"])
    out["asset_turnover"] = safe_divide(out["revenue"], out["assets"])
    out["cash_flow_to_debt"] = safe_divide(out["operating_cash_flow"], out["liabilities"])
    out["size_log_assets"] = np.log(out["assets"].where(out["assets"] > 0))

    return out


def train_default_models(df: pd.DataFrame, target_col: str = "distress_flag"):
    """
    Train baseline logistic regression and gradient boosting models.
    """
    feature_cols = [
        "roe",
        "debt_ratio",
        "equity_ratio",
        "operating_margin",
        "net_income_margin",
        "asset_turnover",
        "cash_flow_to_debt",
        "size_log_assets",
    ]

    model_df = build_features(df)

    missing = [c for c in feature_cols + [target_col] if c not in model_df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    X = model_df[feature_cols]
    y = model_df[target_col].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.25,
        random_state=42,
        stratify=y
    )

    logistic_model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=2000, class_weight="balanced"))
    ])

    gboost_model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("clf", GradientBoostingClassifier(random_state=42))
    ])

    logistic_model.fit(X_train, y_train)
    gboost_model.fit(X_train, y_train)

    log_pred = logistic_model.predict(X_test)
    log_prob = logistic_model.predict_proba(X_test)[:, 1]

    gb_pred = gboost_model.predict(X_test)
    gb_prob = gboost_model.predict_proba(X_test)[:, 1]

    print("\n=== Logistic Regression ===")
    print("ROC AUC:", roc_auc_score(y_test, log_prob))
    print("Confusion Matrix:\n", confusion_matrix(y_test, log_pred))
    print(classification_report(y_test, log_pred, digits=3))

    print("\n=== Gradient Boosting ===")
    print("ROC AUC:", roc_auc_score(y_test, gb_prob))
    print("Confusion Matrix:\n", confusion_matrix(y_test, gb_pred))
    print(classification_report(y_test, gb_pred, digits=3))

    scored = model_df.copy()
    scored["pd_logistic"] = logistic_model.predict_proba(X)[:, 1]
    scored["pd_gboost"] = gboost_model.predict_proba(X)[:, 1]

    return logistic_model, gboost_model, scored


if __name__ == "__main__":
    # Example: replace with your real CSV path
    df = pd.read_csv("jp_annual_financials.csv")

    # Expected target column already present:
    # distress_flag = 1 if distressed/defaulted, else 0
    logistic_model, gboost_model, scored = train_default_models(df)

    print("\nTop highest-risk companies:")
    cols_to_show = [c for c in [
        "filer_name_jp", "filer_name_en", "ticker",
        "pd_logistic", "pd_gboost", "distress_flag"
    ] if c in scored.columns]

    print(
        scored.sort_values("pd_gboost", ascending=False)[cols_to_show].head(20)
    )

    scored.to_csv("jp_default_risk_scored.csv", index=False, encoding="utf-8-sig")