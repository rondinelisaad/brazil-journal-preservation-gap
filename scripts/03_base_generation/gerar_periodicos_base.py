"""
gerar_periodicos_base_corrigido_v3.py

Consolida `preservacao_titledb.csv` em uma entidade de periódico mais robusta,
resolvendo fragmentação por:
- ISSN isolado
- eISSN isolado
- ISSN + eISSN
- variações leves de título/editora

Estratégia:
1. cria blocos por título normalizado
2. dentro de cada bloco, cria componentes conectados por ISSN/eISSN
3. se um registro não tem ISSN nem eISSN, cai em um componente próprio por título

Saídas:
- periodicos_base_corrigido_v3.csv
- mapa_periodicos_consolidados_v3.csv
"""

from __future__ import annotations

import argparse
import hashlib
import re
import unicodedata
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


def hash_id(prefix: str, parts: Iterable[Optional[str]], size: int = 16) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:size]
    return f"{prefix}{digest.upper()}"


def normalize_text(value):
    if pd.isna(value) or value is None:
        return None
    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"\s+", " ", value)
    return value or None


def normalize_issn(value):
    if pd.isna(value) or value is None:
        return None
    value = str(value).strip().upper().replace(" ", "").replace("-", "")
    if len(value) != 8:
        return None
    return f"{value[:4]}-{value[4:]}"


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


def choose_best_url(group: pd.DataFrame):
    g = group.copy()
    g["year_declared_num"] = pd.to_numeric(g["year_declared"], errors="coerce")
    g = g.sort_values(["year_declared_num"], ascending=False, na_position="last")
    return first_non_null(g["base_url"]), first_non_null(g["base_url_domain"])


def infer_platform(plugin_name):
    if pd.isna(plugin_name) or plugin_name is None:
        return pd.NA
    s = str(plugin_name).lower()
    if "ojs3" in s:
        return "OJS3"
    if "ojs2" in s:
        return "OJS2"
    if "ojs" in s:
        return "OJS"
    return pd.NA


def build_observacao(group: pd.DataFrame):
    anos = pd.to_numeric(group["year_declared"], errors="coerce").dropna()
    entradas = len(group)
    min_ano = int(anos.min()) if not anos.empty else ""
    max_ano = int(anos.max()) if not anos.empty else ""
    issn_vals = sorted({x for x in group["issn_titledb_norm"].dropna().tolist() if x})
    eissn_vals = sorted({x for x in group["eissn_titledb_norm"].dropna().tolist() if x})
    return (
        f"gerado a partir do preservacao_titledb consolidado; "
        f"entradas={entradas}; anos={min_ano}-{max_ano}; "
        f"issn={','.join(issn_vals) if issn_vals else ''}; "
        f"eissn={','.join(eissn_vals) if eissn_vals else ''}"
    )


def connected_components_for_title(g: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Dentro de um mesmo título normalizado, cria componentes conectados por ISSN/eISSN.
    Se o registro não tem nenhum identificador, ele vira componente isolado.
    """
    rows = g.reset_index(drop=False).rename(columns={"index": "_orig_index"}).copy()

    # mapa nó -> linhas
    node_to_rows = defaultdict(set)
    row_to_nodes = defaultdict(set)

    for i, row in rows.iterrows():
        nodes = set()
        a = row.get("issn_titledb_norm")
        b = row.get("eissn_titledb_norm")
        if pd.notna(a) and a:
            nodes.add(str(a))
        if pd.notna(b) and b:
            nodes.add(str(b))
        row_to_nodes[i] = nodes
        for n in nodes:
            node_to_rows[n].add(i)

    visited_rows = set()
    components = []

    for i in rows.index:
        if i in visited_rows:
            continue

        if not row_to_nodes[i]:
            visited_rows.add(i)
            components.append(rows.loc[[i]].copy())
            continue

        q = deque([i])
        current_rows = set()

        while q:
            r = q.popleft()
            if r in current_rows:
                continue
            current_rows.add(r)
            for node in row_to_nodes[r]:
                for other in node_to_rows[node]:
                    if other not in current_rows:
                        q.append(other)

        visited_rows.update(current_rows)
        components.append(rows.loc[sorted(current_rows)].copy())

    return components


def consolidate_component(comp: pd.DataFrame):
    titulo_norm = first_non_null(comp["journal_title_norm"])
    titulo = first_non_null(comp["journal_title_titledb"])
    publisher_norm = first_non_null(comp["publisher_norm"])
    publisher = first_non_null(comp["publisher_titledb"])

    issn_imp_norm = first_non_null(comp["issn_titledb_norm"])
    eissn_norm = first_non_null(comp["eissn_titledb_norm"])

    # tenta recuperar forma original correspondente
    issn_imp = first_non_null(comp.loc[comp["issn_titledb_norm"] == issn_imp_norm, "issn_titledb"]) if pd.notna(issn_imp_norm) else pd.NA
    eissn = first_non_null(comp.loc[comp["eissn_titledb_norm"] == eissn_norm, "eissn_titledb"]) if pd.notna(eissn_norm) else pd.NA

    plugin_name = first_non_null(comp["plugin_name"])
    plataforma = infer_platform(plugin_name)

    url_principal, dominio_url = choose_best_url(comp)

    periodico_id = hash_id(
        "PER_",
        [titulo_norm, issn_imp_norm, eissn_norm]
    )

    row = {
        "periodico_id": periodico_id,
        "titulo_principal": titulo,
        "titulo_abreviado": pd.NA,
        "titulo_norm": titulo_norm,
        "issn_impresso": issn_imp,
        "issn_eletronico": eissn,
        "issn_l": pd.NA,
        "issn_impresso_norm": issn_imp_norm,
        "issn_eletronico_norm": eissn_norm,
        "issn_l_norm": pd.NA,
        "publisher_nome": publisher,
        "publisher_norm": publisher_norm,
        "pais": "Brasil",
        "area_tematica": pd.NA,
        "plataforma_editorial": plataforma,
        "url_principal_atual": url_principal,
        "dominio_url_principal": dominio_url,
        "fonte_url_principal": "titledb",
        "ativo_editorial_recente": pd.NA,
        "ano_ultimo_registro_editorial": pd.NA,
        "tem_doaj": False,
        "tem_latindex": False,
        "tem_titledb": True,
        "observacao_match": build_observacao(comp),
    }
    return row


def consolidar_periodicos(df: pd.DataFrame):
    for col in [
        "periodico_id", "plugin_name", "journal_title_titledb", "journal_title_norm",
        "publisher_titledb", "publisher_norm", "issn_titledb", "eissn_titledb",
        "issn_titledb_norm", "eissn_titledb_norm", "base_url", "base_url_domain",
        "year_declared", "titledb_entry_id"
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    base = df.copy()
    base["journal_title_norm"] = base["journal_title_norm"].fillna(base["journal_title_titledb"].map(normalize_text))
    base["publisher_norm"] = base["publisher_norm"].fillna(base["publisher_titledb"].map(normalize_text))
    base["issn_titledb_norm"] = base["issn_titledb_norm"].fillna(base["issn_titledb"].map(normalize_issn))
    base["eissn_titledb_norm"] = base["eissn_titledb_norm"].fillna(base["eissn_titledb"].map(normalize_issn))

    periodicos = []
    mapa = []

    # 1º nível: agrupa por título normalizado
    for titulo_norm, g in base.groupby("journal_title_norm", dropna=False):
        components = connected_components_for_title(g)

        for comp in components:
            novo = consolidate_component(comp)
            periodicos.append(novo)

            for _, row in comp.iterrows():
                mapa.append({
                    "periodico_id_original": row.get("periodico_id", pd.NA),
                    "periodico_id_corrigido": novo["periodico_id"],
                    "titledb_entry_id": row.get("titledb_entry_id", pd.NA),
                    "journal_title_titledb": row.get("journal_title_titledb", pd.NA),
                    "journal_title_norm": row.get("journal_title_norm", pd.NA),
                    "issn_titledb_norm": row.get("issn_titledb_norm", pd.NA),
                    "eissn_titledb_norm": row.get("eissn_titledb_norm", pd.NA),
                    "publisher_norm": row.get("publisher_norm", pd.NA),
                })

    periodicos_df = pd.DataFrame(periodicos).drop_duplicates(subset=["periodico_id"]).reset_index(drop=True)
    mapa_df = pd.DataFrame(mapa).reset_index(drop=True)

    return periodicos_df, mapa_df


def main():
    parser = argparse.ArgumentParser(description="Gera periodicos_base corrigido v3 a partir de preservacao_titledb.csv")
    parser.add_argument("--input", type=Path, required=True, help="Arquivo preservacao_titledb.csv")
    parser.add_argument("--outdir", type=Path, default=Path("."))
    args = parser.parse_args()

    df = pd.read_csv(args.input, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    args.outdir.mkdir(parents=True, exist_ok=True)

    periodicos, mapa = consolidar_periodicos(df)

    periodicos.to_csv(args.outdir / "periodicos_base_corrigido_v3.csv", index=False, encoding="utf-8")
    mapa.to_csv(args.outdir / "mapa_periodicos_consolidados_v3.csv", index=False, encoding="utf-8")

    print(f"preservacao_titledb.csv: {len(df)} entradas")
    print(f"periodicos_base_corrigido_v3.csv: {len(periodicos)} periodicos consolidados")
    print(f"mapa_periodicos_consolidados_v3.csv: {len(mapa)} linhas de rastreabilidade")
    print(f"Arquivos gravados em: {args.outdir.resolve()}")


if __name__ == "__main__":
    main()
