#!/usr/bin/env python3
"""
06_analysis.py

Reproduces the main analytical summaries from the manual validation stage
and selected cross-checks with the risk diagnosis dataset.

Usage examples:
    python3 06_analysis.py
    python3 06_analysis.py --data-dir data/processed --outdir outputs/tables

Expected input files:
    - diagnostico_risco.csv
    - infraestrutura_url.csv
    - periodicos_enriquecido.csv
    - periodicos_base_corrigido.csv
    - validacao_manual.csv

Notes:
    - The script assumes validacao_manual.csv is semicolon-separated.
    - It can be executed from the repository root or from inside data/processed.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import pandas as pd


REQUIRED_FILES = {
    "diagnostico_risco": "diagnostico_risco.csv",
    "infraestrutura_url": "infraestrutura_url.csv",
    "periodicos_enriquecido": "periodicos_enriquecido.csv",
    "periodicos_base_corrigido": "periodicos_base_corrigido.csv",
    "validacao_manual": "validacao_manual.csv",
}


BOOL_COLS = [
    "url_real_correta_identificada",
    "periodico_ativo_manual",
    "evidencia_volume_recente",
    "mudanca_url_observada",
    "redirecionamento_observado",
    "inconsistencia_issn",
    "resolucao_direta_real_identificada",
]

CAT_COLS = [
    "classificacao_validacao",
    "grupo_validacao",
    "qualidade_url_origem",
    "recuperacao_assistida",
    "problema_infraestrutura",
    "correspondencia_periodico",
    "observacao_validacao",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reproduce analytical summaries from the journal validation workflow."
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Directory containing processed CSV files. Default: data/processed",
    )
    parser.add_argument(
        "--outdir",
        default="outputs/tables",
        help="Directory where summary tables will be saved. Default: outputs/tables",
    )
    return parser.parse_args()


def resolve_data_dir(data_dir_str: str) -> Path:
    """
    Allows execution either from repository root or from inside data/processed.
    """
    candidate = Path(data_dir_str)
    if candidate.exists():
        return candidate

    if Path(".").resolve().name == "processed":
        return Path(".")

    raise FileNotFoundError(
        f"Could not find data directory: {data_dir_str}. "
        "Run from repository root or specify --data-dir explicitly."
    )


def load_files(data_dir: Path) -> Dict[str, pd.DataFrame]:
    missing = [fname for fname in REQUIRED_FILES.values() if not (data_dir / fname).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing required files in {data_dir}: {', '.join(missing)}"
        )

    dfs = {
        "diagnostico_risco": pd.read_csv(data_dir / REQUIRED_FILES["diagnostico_risco"]),
        "infraestrutura_url": pd.read_csv(data_dir / REQUIRED_FILES["infraestrutura_url"]),
        "periodicos_enriquecido": pd.read_csv(data_dir / REQUIRED_FILES["periodicos_enriquecido"]),
        "periodicos_base_corrigido": pd.read_csv(data_dir / REQUIRED_FILES["periodicos_base_corrigido"]),
        "validacao_manual": pd.read_csv(data_dir / REQUIRED_FILES["validacao_manual"], sep=";"),
    }
    return dfs


def normalize_validation_df(val: pd.DataFrame) -> pd.DataFrame:
    val = val.copy()

    for col in BOOL_COLS:
        if col in val.columns:
            val[col] = (
                val[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({"true": True, "false": False, "nan": pd.NA, "": pd.NA})
            )

    for col in CAT_COLS:
        if col in val.columns:
            val[col] = val[col].astype(str).str.strip().str.lower()

    if "grupo_validacao" in val.columns:
        val["grupo_validacao"] = val["grupo_validacao"].str.upper()

    return val


def pct_series(series: pd.Series) -> pd.Series:
    return (series.value_counts(normalize=True, dropna=False) * 100).round(2)


def crosstab_count(df: pd.DataFrame, row: str, col: str) -> pd.DataFrame:
    return pd.crosstab(df[row], df[col], dropna=False)


def crosstab_pct(df: pd.DataFrame, row: str, col: str) -> pd.DataFrame:
    return (pd.crosstab(df[row], df[col], normalize="index", dropna=False) * 100).round(2)


def save_table(df: pd.DataFrame | pd.Series, outdir: Path, name: str) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"{name}.csv"
    if isinstance(df, pd.Series):
        df.to_csv(path, header=True)
    else:
        df.to_csv(path)


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    args = parse_args()
    data_dir = resolve_data_dir(args.data_dir)
    outdir = Path(args.outdir)

    dfs = load_files(data_dir)
    diag = dfs["diagnostico_risco"]
    val = normalize_validation_df(dfs["validacao_manual"])

    print_header("FILES LOADED")
    for key, df in dfs.items():
        print(f"{key}: shape={df.shape}")

    print_header("OVERALL VALIDATION OUTCOME")
    outcome_counts = val["classificacao_validacao"].value_counts(dropna=False)
    outcome_pct = pct_series(val["classificacao_validacao"])
    print(outcome_counts)
    print()
    print(outcome_pct)

    print_header("GROUP x VALIDATION OUTCOME")
    grp_outcome = crosstab_count(val, "grupo_validacao", "classificacao_validacao")
    grp_outcome_pct = crosstab_pct(val, "grupo_validacao", "classificacao_validacao")
    print(grp_outcome)
    print()
    print(grp_outcome_pct)

    print_header("URL QUALITY")
    url_quality_counts = val["qualidade_url_origem"].value_counts(dropna=False)
    url_quality_pct = pct_series(val["qualidade_url_origem"])
    print(url_quality_counts)
    print()
    print(url_quality_pct)

    print_header("CORRECT URL IDENTIFIED")
    correct_url_counts = val["url_real_correta_identificada"].value_counts(dropna=False)
    correct_url_pct = pct_series(val["url_real_correta_identificada"])
    print(correct_url_counts)
    print()
    print(correct_url_pct)

    print_header("DIRECT RESOLUTION IDENTIFIED")
    direct_resolution_counts = val["resolucao_direta_real_identificada"].value_counts(dropna=False)
    direct_resolution_pct = pct_series(val["resolucao_direta_real_identificada"])
    print(direct_resolution_counts)
    print()
    print(direct_resolution_pct)

    print_header("ASSISTED RECOVERY")
    recovery_counts = val["recuperacao_assistida"].value_counts(dropna=False)
    recovery_pct = pct_series(val["recuperacao_assistida"])
    print(recovery_counts)
    print()
    print(recovery_pct)

    print_header("URL QUALITY x RECOVERY COMPLEXITY")
    qxrec = crosstab_count(val, "qualidade_url_origem", "recuperacao_assistida")
    qxrec_pct = crosstab_pct(val, "qualidade_url_origem", "recuperacao_assistida")
    print(qxrec)
    print()
    print(qxrec_pct)

    print_header("INFRASTRUCTURE ISSUE x VALIDATION OUTCOME")
    infra_outcome = crosstab_count(val, "classificacao_validacao", "problema_infraestrutura")
    infra_outcome_pct = crosstab_pct(val, "classificacao_validacao", "problema_infraestrutura")
    print(infra_outcome)
    print()
    print(infra_outcome_pct)

    print_header("URL QUALITY x VALIDATION OUTCOME")
    quality_outcome = crosstab_count(val, "qualidade_url_origem", "classificacao_validacao")
    quality_outcome_pct = crosstab_pct(val, "qualidade_url_origem", "classificacao_validacao")
    print(quality_outcome)
    print()
    print(quality_outcome_pct)

    print_header("ISSN INCONSISTENCY x VALIDATION OUTCOME")
    if "inconsistencia_issn" in val.columns:
        issn_outcome = crosstab_count(val, "inconsistencia_issn", "classificacao_validacao")
        print(issn_outcome)
    else:
        issn_outcome = pd.DataFrame()
        print("Column 'inconsistencia_issn' not found.")

    print_header("RECOVERY COMPLEXITY x VALIDATION OUTCOME")
    recovery_outcome = crosstab_count(val, "recuperacao_assistida", "classificacao_validacao")
    recovery_outcome_pct = crosstab_pct(val, "recuperacao_assistida", "classificacao_validacao")
    print(recovery_outcome)
    print()
    print(recovery_outcome_pct)

    print_header("MERGE WITH RISK DIAGNOSIS")
    if "periodico_id" in diag.columns and "periodico_id" in val.columns:
        val_diag = val.merge(diag, on="periodico_id", how="left")
        print(f"Merged shape: {val_diag.shape}")

        risk_cols = [c for c in val_diag.columns if "risco" in c.lower() or "motivo" in c.lower()]
        print("Risk-related columns:", risk_cols)

        if "grau_risco" in val_diag.columns:
            print("\nclassificacao_validacao x grau_risco")
            print(pd.crosstab(val_diag["classificacao_validacao"], val_diag["grau_risco"], dropna=False))

        if "risco_estrutural" in val_diag.columns:
            print("\nclassificacao_validacao x risco_estrutural")
            print(pd.crosstab(val_diag["classificacao_validacao"], val_diag["risco_estrutural"], dropna=False))

        if "motivo_risco" in val_diag.columns:
            print("\nclassificacao_validacao x motivo_risco")
            print(pd.crosstab(val_diag["classificacao_validacao"], val_diag["motivo_risco"], dropna=False))
    else:
        val_diag = pd.DataFrame()
        print("Could not merge validation with diagnosis: missing periodico_id.")

    print_header("CONSISTENCY CHECKS")

    check_inconclusive_but_correct = val[
        (val["classificacao_validacao"] == "inconclusivo")
        & (val["url_real_correta_identificada"] == True)
    ]

    check_confirmed_but_invalid = val[
        (val["classificacao_validacao"] == "confirmado")
        & (val["qualidade_url_origem"] == "invalida")
    ]

    print("Inconclusive but correct URL identified:", len(check_inconclusive_but_correct))
    print(check_inconclusive_but_correct)

    print("\nConfirmed but invalid URL:", len(check_confirmed_but_invalid))
    print(check_confirmed_but_invalid)

    print_header("AUTOMATIC SUMMARY")
    n_total = len(val)
    n_confirmado = (val["classificacao_validacao"] == "confirmado").sum()
    n_inconclusivo = (val["classificacao_validacao"] == "inconclusivo").sum()

    qa = val["qualidade_url_origem"].value_counts(dropna=False).to_dict()
    rec = val["recuperacao_assistida"].value_counts(dropna=False).to_dict()

    print(f"""
Cases analyzed: {n_total}
Confirmed: {n_confirmado} ({n_confirmado / n_total * 100:.2f}%)
Inconclusive: {n_inconclusivo} ({n_inconclusivo / n_total * 100:.2f}%)

URL quality:
- direta: {qa.get('direta', 0)}
- indireta: {qa.get('indireta', 0)}
- invalida: {qa.get('invalida', 0)}

Assisted recovery:
- baixa_complexidade: {rec.get('baixa_complexidade', 0)}
- media_complexidade: {rec.get('media_complexidade', 0)}
- alta_complexidade: {rec.get('alta_complexidade', 0)}
""")

    print_header("SAVING TABLES")
    save_table(outcome_counts, outdir, "analysis_outcome_counts")
    save_table(outcome_pct, outdir, "analysis_outcome_pct")
    save_table(grp_outcome, outdir, "analysis_group_x_outcome_counts")
    save_table(grp_outcome_pct, outdir, "analysis_group_x_outcome_pct")
    save_table(url_quality_counts, outdir, "analysis_url_quality_counts")
    save_table(url_quality_pct, outdir, "analysis_url_quality_pct")
    save_table(correct_url_counts, outdir, "analysis_correct_url_counts")
    save_table(correct_url_pct, outdir, "analysis_correct_url_pct")
    save_table(direct_resolution_counts, outdir, "analysis_direct_resolution_counts")
    save_table(direct_resolution_pct, outdir, "analysis_direct_resolution_pct")
    save_table(recovery_counts, outdir, "analysis_recovery_counts")
    save_table(recovery_pct, outdir, "analysis_recovery_pct")
    save_table(qxrec, outdir, "analysis_url_quality_x_recovery_counts")
    save_table(qxrec_pct, outdir, "analysis_url_quality_x_recovery_pct")
    save_table(infra_outcome, outdir, "analysis_infrastructure_x_outcome_counts")
    save_table(infra_outcome_pct, outdir, "analysis_infrastructure_x_outcome_pct")
    save_table(quality_outcome, outdir, "analysis_url_quality_x_outcome_counts")
    save_table(quality_outcome_pct, outdir, "analysis_url_quality_x_outcome_pct")
    save_table(issn_outcome, outdir, "analysis_issn_x_outcome_counts")
    save_table(recovery_outcome, outdir, "analysis_recovery_x_outcome_counts")
    save_table(recovery_outcome_pct, outdir, "analysis_recovery_x_outcome_pct")

    if not val_diag.empty:
        if "grau_risco" in val_diag.columns:
            save_table(
                pd.crosstab(val_diag["classificacao_validacao"], val_diag["grau_risco"], dropna=False),
                outdir,
                "analysis_outcome_x_grau_risco_counts",
            )
        if "risco_estrutural" in val_diag.columns:
            save_table(
                pd.crosstab(val_diag["classificacao_validacao"], val_diag["risco_estrutural"], dropna=False),
                outdir,
                "analysis_outcome_x_risco_estrutural_counts",
            )
        if "motivo_risco" in val_diag.columns:
            save_table(
                pd.crosstab(val_diag["classificacao_validacao"], val_diag["motivo_risco"], dropna=False),
                outdir,
                "analysis_outcome_x_motivo_risco_counts",
            )

    print(f"Tables saved to: {outdir.resolve()}")


if __name__ == "__main__":
    main()
