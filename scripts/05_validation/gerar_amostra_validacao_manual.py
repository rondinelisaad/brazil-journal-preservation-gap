"""
gerar_amostra_validacao_manual.py

Gera amostra estratificada para validação manual a partir de diagnostico_risco.csv.

Grupos:
- A: tem_dados_editoriais = False e risco_estrutural = True
- B: tem_dados_editoriais = False e risco_estrutural = False

Saídas:
- amostra_validacao_manual.csv
- planilha_validacao_manual.csv

Uso:
python3 gerar_amostra_validacao_manual.py \
  --input saida/diagnostico_risco.csv \
  --outdir ./saida \
  --n-a 30 \
  --n-b 30 \
  --seed 42
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


COLUNAS_PLANILHA = [
    "periodico_id",
    "titulo_principal",
    "issn_impresso",
    "issn_eletronico",
    "grupo_validacao",
    "fonte_consultada",
    "url_funcional_identificada",
    "fonte_url_avaliada",
    "url_real_correta_identificada",
    "periodico_ativo_manual",
    "ultimo_ano_editorial_manual",
    "evidencia_volume_recente",
    "mudanca_url_observada",
    "redirecionamento_observado",
    "inconsistencia_issn",
    "correspondencia_periodico",
    "problema_infraestrutura",
    "qualidade_url_origem",
    "resolucao_direta_real_identificada",
    "recuperacao_assistida",
    "observacao_validacao",
    "classificacao_validacao",
]


def bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col]
    mapping = {
        "true": True, "false": False,
        "True": True, "False": False,
        "1": True, "0": False,
        1: True, 0: False,
        True: True, False: False,
    }
    return s.map(lambda x: mapping.get(x, x)).astype("boolean")


def sample_group(df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    n_eff = min(n, len(df))
    return df.sample(n=n_eff, random_state=seed).copy()


def main():
    parser = argparse.ArgumentParser(description="Gera amostra para validação manual")
    parser.add_argument("--input", type=Path, required=True, help="diagnostico_risco.csv")
    parser.add_argument("--outdir", type=Path, default=Path("."))
    parser.add_argument("--n-a", type=int, default=30, help="Tamanho da amostra do grupo A")
    parser.add_argument("--n-b", type=int, default=30, help="Tamanho da amostra do grupo B")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = pd.read_csv(args.input, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    args.outdir.mkdir(parents=True, exist_ok=True)

    # Garantia mínima das colunas necessárias
    required = ["periodico_id", "titulo_principal", "issn_impresso", "issn_eletronico", "tem_dados_editoriais", "risco_estrutural"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Arquivo de entrada sem colunas obrigatórias: {missing}")

    df["tem_dados_editoriais"] = bool_series(df, "tem_dados_editoriais")
    df["risco_estrutural"] = bool_series(df, "risco_estrutural")

    grupo_a = df[(df["tem_dados_editoriais"] == False) & (df["risco_estrutural"] == True)].copy()
    grupo_b = df[(df["tem_dados_editoriais"] == False) & (df["risco_estrutural"] == False)].copy()

    amostra_a = sample_group(grupo_a, args.n_a, args.seed)
    amostra_b = sample_group(grupo_b, args.n_b, args.seed)

    amostra_a["grupo_validacao"] = "A"
    amostra_b["grupo_validacao"] = "B"

    amostra = pd.concat([amostra_a, amostra_b], ignore_index=True)

    # CSV resumido da amostra sorteada
    cols_saida = ["periodico_id", "titulo_principal", "issn_impresso", "issn_eletronico", "grupo_validacao", "tem_dados_editoriais", "risco_estrutural"]
    for col in cols_saida:
        if col not in amostra.columns:
            amostra[col] = pd.NA

    amostra[cols_saida].to_csv(args.outdir / "amostra_validacao_manual.csv", index=False, encoding="utf-8")

    # Planilha de preenchimento manual
    planilha = amostra[["periodico_id", "titulo_principal", "issn_impresso", "issn_eletronico", "grupo_validacao"]].copy()
    for col in COLUNAS_PLANILHA:
        if col not in planilha.columns:
            planilha[col] = pd.NA

    planilha = planilha[COLUNAS_PLANILHA]
    planilha.to_csv(args.outdir / "planilha_validacao_manual.csv", index=False, encoding="utf-8")

    # Pequeno resumo
    resumo = pd.DataFrame([
        {"grupo": "A", "universo": len(grupo_a), "amostra": len(amostra_a)},
        {"grupo": "B", "universo": len(grupo_b), "amostra": len(amostra_b)},
        {"grupo": "TOTAL", "universo": len(grupo_a) + len(grupo_b), "amostra": len(amostra)},
    ])
    resumo.to_csv(args.outdir / "resumo_amostra_validacao.csv", index=False, encoding="utf-8")

    print("Arquivos gerados:")
    print(f"- {args.outdir / 'amostra_validacao_manual.csv'}")
    print(f"- {args.outdir / 'planilha_validacao_manual.csv'}")
    print(f"- {args.outdir / 'resumo_amostra_validacao.csv'}")
    print()
    print("Tamanhos:")
    print(resumo.to_string(index=False))


if __name__ == "__main__":
    main()
