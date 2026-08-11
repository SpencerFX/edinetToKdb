import sys
import os
import time
import calendar
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import edinet_tools


CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.loadConfig import load_config


def parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def daterange(start_date: date, end_date: date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def collect_semi_annual_statements_by_date(
    date_str: str,
    sleep_seconds: float,
    doc_type_codes: list[str]
) -> pd.DataFrame:
    rows = []

    for doc_type_code in doc_type_codes:
        docs = edinet_tools.documents(date_str, doc_type=doc_type_code)
        print(f"{date_str} doc_type={doc_type_code}: {len(docs)} filings")

        for doc in docs:
            try:
                report = doc.parse()

                row = {
                    "doc_id": getattr(doc, "doc_id", None),
                    "filing_datetime": getattr(doc, "filing_datetime", None),
                    "doc_type_name": getattr(doc, "doc_type_name", None),
                    "doc_type_code_used": doc_type_code,

                    "filer_name": getattr(report, "filer_name", None),
                    "ticker": getattr(report, "ticker", None),
                    "fiscal_year_end": getattr(report, "fiscal_year_end", None),
                    "accounting_standard": getattr(report, "accounting_standard", None),

                    "net_sales": getattr(report, "net_sales", None),
                    "operating_income": getattr(report, "operating_income", None),
                    "net_income": getattr(report, "net_income", None),

                    "assets": getattr(report, "assets", None),
                    "liabilities": getattr(report, "liabilities", None),
                    "equity": getattr(report, "equity", None),

                    "operating_cash_flow": getattr(report, "operating_cash_flow", None),
                    "investing_cash_flow": getattr(report, "investing_cash_flow", None),
                    "financing_cash_flow": getattr(report, "financing_cash_flow", None),

                    "roe": getattr(report, "roe", None),
                    "equity_ratio": getattr(report, "equity_ratio", None),
                }

                if any(pd.notna(row.get(col)) for col in ["filer_name", "ticker", "net_sales", "assets"]):
                    rows.append(row)

            except Exception as exc:
                print(
                    f"[WARN] parse failed "
                    f"doc_type={doc_type_code} "
                    f"doc_id={getattr(doc, 'doc_id', None)}: {exc}"
                )

            time.sleep(sleep_seconds)

    return pd.DataFrame(rows)


def load_translations(translations_csv: str) -> pd.DataFrame:
    translations = pd.read_csv(
        translations_csv,
        encoding="utf-8-sig",
        engine="python",
        header=None,
        names=["filer_name_jp", "filer_name_en"],
        sep=",",
        usecols=[0, 1]
    )

    translations.columns = ["filer_name_jp", "filer_name_en"]
    return translations


def reposition_filer_name_en(df: pd.DataFrame) -> pd.DataFrame:
    if "filer_name_en" not in df.columns or "filer_name" not in df.columns:
        return df

    col_list = df.columns.tolist()

    if "filer_name_en" in col_list:
        col_list.remove("filer_name_en")

    filer_name_idx = col_list.index("filer_name")
    col_list.insert(filer_name_idx + 1, "filer_name_en")

    return df[col_list]


def build_output_filename(output_prefix: str, start_date: date, end_date: date) -> str:
    if start_date == end_date:
        return f"{output_prefix}_{start_date:%Y%m%d}.csv"
    return f"{output_prefix}_{start_date:%Y%m%d}_{end_date:%Y%m%d}.csv"


def main():
    parser = argparse.ArgumentParser(
        description="Collect EDINET semi-annual statements over a date range."
    )
    parser.add_argument(
        "--start-date",
        required=False,
        help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end-date",
        required=False,
        help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--config",
        required=False,
        default=None,
        help="Optional path to config.toml"
    )

    args = parser.parse_args()

    cfg = load_config(args.config)

    working_dir = cfg["paths"]["working_dir"]
    translations_csv = cfg["paths"]["translations_csv"]
    output_dir = cfg["paths"]["output_dir"]

    sleep_seconds = float(cfg["edinet"]["sleep_seconds"])
    doc_type_codes = [
        str(cfg["edinet"]["doc_type_semi_annual_1"]),
        str(cfg["edinet"]["doc_type_semi_annual_2"]),
    ]

    output_prefix = cfg["files"]["semi_annual_output_prefix"]

    if args.start_date and args.end_date:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
    else:
        start_year = int(cfg["run"]["default_start_year"])
        start_month = int(cfg["run"]["default_start_month"])
        end_year = int(cfg["run"]["default_end_year"])
        end_month = int(cfg["run"]["default_end_month"])

        start_date = date(start_year, start_month, 1)
        end_date = date(end_year, end_month, calendar.monthrange(end_year, end_month)[1])

    if end_date < start_date:
        raise ValueError("end-date must be greater than or equal to start-date")

    os.chdir(working_dir)

    print(f"Processing semi-annual filings from {start_date} to {end_date}")
    print(f"Using doc types: {doc_type_codes}")

    all_frames = []

    for current_date in daterange(start_date, end_date):
        daily_df = collect_semi_annual_statements_by_date(
            date_str=current_date.isoformat(),
            sleep_seconds=sleep_seconds,
            doc_type_codes=doc_type_codes
        )
        all_frames.append(daily_df)

    if all_frames:
        df_all = pd.concat(all_frames, ignore_index=True)
    else:
        df_all = pd.DataFrame()

    print("Collected shape:", df_all.shape)

    translations = load_translations(translations_csv)
    print(translations.head())

    df_final = df_all.merge(
        translations,
        left_on="filer_name",
        right_on="filer_name_jp",
        how="left"
    )

    df_final = df_final.drop(columns=["filer_name_jp"], errors="ignore")
    df_final = reposition_filer_name_en(df_final)

    os.makedirs(output_dir, exist_ok=True)

    output_filename = build_output_filename(
        output_prefix=output_prefix,
        start_date=start_date,
        end_date=end_date
    )
    full_path = os.path.join(output_dir, output_filename)

    df_final.to_csv(full_path, index=False, encoding="utf-8-sig")

    print("Saved to:", full_path)
    print("Final shape:", df_final.shape)


if __name__ == "__main__":
    main()