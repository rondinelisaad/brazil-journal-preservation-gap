"""
aplicar_revisao_periodicos_residuais.py

Aplica uma revisão manual sobre `periodicos_base_corrigido_v3.csv`, a partir de uma planilha
onde o usuário marca quais casos residuais devem ser fundidos ou mantidos separados.

Entradas:
- periodicos_base_corrigido_v3.csv
- revisao_manual_periodicos_residuais.csv

Saídas:
- periodicos_base_revisado.csv
- mapa_revisao_periodicos_residuais.csv

Regras da planilha de revisão:
- `decisao_manual`:
    - `fundir`
    - `manter`
- `grupo_fusao`:
    - identificador textual do grupo que deve ser fundido
    - ex.: G1, G2, G3...
- Para linhas com `decisao_manual = manter`, `grupo_fusao` pode ficar vazio

Uso:
python3 aplicar_revisao_periodicos_residuais.py \
  --periodicos saida/periodicos_base_corrigido_v3.csv \
  --revisao saida/revisao_manual_periodicos_residuais.csv \
  --outdir ./saida
"""

from __future__ import annotations

import argparse
import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


def hash_id(prefix: str, parts: Iterable[Optional[str]], size: int = 16) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:size]
    return f"{prefix}{digest.upper()}"


def first_non_null(series: pd.Series):
    vals = series.dropna()
    return vals.iloc[0] if not vals.empty else pd.NA


def merge_unique_text(series: pd.Series, sep: str = " | "):
    vals = []
    seen = set()
    for v in series.dropna():
        s = str(v).strip()
        if s and s not in seen:
            vals.append(s)
            seen.add(s)
    return sep.join(vals) if vals else pd.NA


def choose_primary_issn(series: pd.Series):
    vals = [v for v in series.dropna().unique().tolist() if str(v).strip()]
    return vals[0] if vals else pd.NA


def consolidate_group(df_group: pd.DataFrame, review_group_id: str):
    titulo_principal = first_non_null(df_group["titulo_principal"])
    titulo_norm = first_non_null(df_group["titulo_norm"])
    publisher_norm = first_non_null(df_group["publisher_norm"])

    issn_impresso = choose_primary_issn(df_group["issn_impresso"])
    issn_eletronico = choose_primary_issn(df_group["issn_eletronico"])
    issn_impresso_norm = choose_primary_issn(df_group["issn_impresso_norm"])
    issn_eletronico_norm = choose_primary_issn(df_group["issn_eletronico_norm"])

    novo_periodico_id = hash_id(
        "PER_",
        [titulo_norm, issn_impresso_norm, issn_eletronico_norm, review_group_id]
    )

    row = {
        "periodico_id": novo_periodico_id,
        "titulo_principal": titulo_principal,
        "titulo_abreviado": first_non_null(df_group["titulo_abreviado"]) if "titulo_abreviado" in df_group.columns else pd.NA,
        "titulo_norm": titulo_norm,
        "issn_impresso": issn_impresso,
        "issn_eletronico": issn_eletronico,
        "issn_l": first_non_null(df_group["issn_l"]) if "issn_l" in df_group.columns else pd.NA,
        "issn_impresso_norm": issn_impresso_norm,
        "issn_eletronico_norm": issn_eletronico_norm,
        "issn_l_norm": first_non_null(df_group["issn_l_norm"]) if "issn_l_norm" in df_group.columns else pd.NA,
        "publisher_nome": first_non_null(df_group["publisher_nome"]) if "publisher_nome" in df_group.columns else pd.NA,
        "publisher_norm": publisher_norm,
        "pais": first_non_null(df_group["pais"]) if "pais" in df_group.columns else pd.NA,
        "area_tematica": merge_unique_text(df_group["area_tematica"]) if "area_tematica" in df_group.columns else pd.NA,
        "plataforma_editorial": first_non_null(df_group["plataforma_editorial"]) if "plataforma_editorial" in df_group.columns else pd.NA,
        "url_principal_atual": first_non_null(df_group["url_principal_atual"]) if "url_principal_atual" in df_group.columns else pd.NA,
        "dominio_url_principal": first_non_null(df_group["dominio_url_principal"]) if "dominio_url_principal" in df_group.columns else pd.NA,
        "fonte_url_principal": merge_unique_text(df_group["fonte_url_principal"]) if "fonte_url_principal" in df_group.columns else pd.NA,
        "ativo_editorial_recente": first_non_null(df_group["ativo_editorial_recente"]) if "ativo_editorial_recente" in df_group.columns else pd.NA,
        "ano_ultimo_registro_editorial": first_non_null(df_group["ano_ultimo_registro_editorial"]) if "ano_ultimo_registro_editorial" in df_group.columns else pd.NA,
        "tem_doaj": first_non_null(df_group["tem_doaj"]) if "tem_doaj" in df_group.columns else False,
        "tem_latindex": first_non_null(df_group["tem_latindex"]) if "tem_latindex" in df_group.columns else False,
        "tem_titledb": first_non_null(df_group["tem_titledb"]) if "tem_titledb" in df_group.columns else True,
        "observacao_match": f"registro consolidado manualmente no grupo {review_group_id}",
    }
    return row


def main():
    parser = argparse.ArgumentParser(description="Aplica revisão manual sobre periodicos_base_corrigido_v3.csv")
    parser.add_argument("--periodicos", type=Path, required=True)
    parser.add_argument("--revisao", type=Path, required=True)
    parser.add_argument("--outdir", type=Path, default=Path("."))
    args = parser.parse_args()

    df = pd.read_csv(args.periodicos, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    #rev = pd.read_csv(args.revisao, sep=";", dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])

    try:
       rev = pd.read_csv(args.revisao, sep=";", dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
       if len(rev.columns) == 1:
           rev = pd.read_csv(args.revisao, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    except Exception:
       rev = pd.read_csv(args.revisao, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])

    args.outdir.mkdir(parents=True, exist_ok=True)

    required_rev = ["periodico_id", "decisao_manual", "grupo_fusao"]
    missing = [c for c in required_rev if c not in rev.columns]
    if missing:
        raise ValueError(f"Planilha de revisão sem colunas obrigatórias: {missing}")

    rev["decisao_manual"] = rev["decisao_manual"].astype("string").str.strip().str.lower()

    # separa linhas revisadas
    ids_revisados = set(rev["periodico_id"].dropna().astype(str))
    base_nao_revisada = df[~df["periodico_id"].astype(str).isin(ids_revisados)].copy()

    # mantém casos explicitamente marcados como manter
    rev_manter = rev[rev["decisao_manual"] == "manter"].copy()
    ids_manter = set(rev_manter["periodico_id"].dropna().astype(str))
    base_manter = df[df["periodico_id"].astype(str).isin(ids_manter)].copy()

    # funde grupos
    rev_fundir = rev[rev["decisao_manual"] == "fundir"].copy()
    grupos_validos = rev_fundir["grupo_fusao"].dropna().astype(str).str.strip()
    grupos_validos = [g for g in grupos_validos.unique().tolist() if g]

    rows_fundidas = []
    mapa = []

    for grupo in grupos_validos:
        ids_grupo = rev_fundir.loc[rev_fundir["grupo_fusao"].astype("string").str.strip() == grupo, "periodico_id"].dropna().astype(str).tolist()
        bloco = df[df["periodico_id"].astype(str).isin(ids_grupo)].copy()
        if bloco.empty:
            continue

        novo = consolidate_group(bloco, grupo)
        rows_fundidas.append(novo)

        for pid in ids_grupo:
            mapa.append({
                "periodico_id_original": pid,
                "periodico_id_revisado": novo["periodico_id"],
                "grupo_fusao": grupo,
                "decisao_manual": "fundir",
            })

    # mapa dos mantidos
    for pid in ids_manter:
        mapa.append({
            "periodico_id_original": pid,
            "periodico_id_revisado": pid,
            "grupo_fusao": pd.NA,
            "decisao_manual": "manter",
        })

    base_fundida = pd.DataFrame(rows_fundidas)
    final = pd.concat([base_nao_revisada, base_manter, base_fundida], ignore_index=True)

    # remove eventual duplicação residual por periodico_id
    final = final.drop_duplicates(subset=["periodico_id"]).reset_index(drop=True)
    mapa_df = pd.DataFrame(mapa).reset_index(drop=True)

    final.to_csv(args.outdir / "periodicos_base_revisado.csv", index=False, encoding="utf-8")
    mapa_df.to_csv(args.outdir / "mapa_revisao_periodicos_residuais.csv", index=False, encoding="utf-8")

    print(f"periodicos_base original: {len(df)}")
    print(f"registros marcados para manter: {len(ids_manter)}")
    print(f"grupos de fusao aplicados: {len(grupos_validos)}")
    print(f"periodicos_base_revisado.csv: {len(final)}")
    print(f"Arquivos gravados em: {args.outdir.resolve()}")


if __name__ == "__main__":
    main()
