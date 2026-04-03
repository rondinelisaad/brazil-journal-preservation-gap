"""
reconciliador_cariniana_atualizado_v2.py

Cruza periodicos_base.csv com OpenAlex, DOAJ, Latindex, infraestrutura_url
e opcionalmente preservacao_titledb.csv.

Inclui:
- tratamento defensivo de pd.NA
- risco_estrutural
- tem_dados_editoriais
- motivo_risco com ausência de dados editoriais
- lógica de atividade editorial separada da cobertura de base
"""

from __future__ import annotations

import argparse
import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd
from urllib.parse import urlparse


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


def extract_domain(url):
    if pd.isna(url) or url is None or not str(url).strip():
        return None
    try:
        return urlparse(str(url).strip()).netloc.lower() or None
    except Exception:
        return None


def safe_bool_series(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True, "false": False,
        "True": True, "False": False,
        "1": True, "0": False,
        1: True, 0: False,
        "sim": True, "nao": False, "não": False,
        "yes": True, "no": False,
    }

    def conv(x):
        if pd.isna(x):
            return pd.NA
        return mapping.get(x, x)

    return series.map(conv).astype("boolean")


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])


def ensure_column(df: pd.DataFrame, col: str, default=pd.NA) -> pd.DataFrame:
    if col not in df.columns:
        df[col] = default
    return df


def prepare_periodicos(df: pd.DataFrame) -> pd.DataFrame:
    for col in [
        "periodico_id", "titulo_principal", "issn_impresso", "issn_eletronico",
        "publisher_nome", "url_principal_atual", "tem_doaj", "tem_latindex", "tem_titledb"
    ]:
        df = ensure_column(df, col)

    df["titulo_norm"] = df.get("titulo_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["titulo_principal"].map(normalize_text)
    )
    df["issn_impresso_norm"] = df.get("issn_impresso_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["issn_impresso"].map(normalize_issn)
    )
    df["issn_eletronico_norm"] = df.get("issn_eletronico_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["issn_eletronico"].map(normalize_issn)
    )
    df["publisher_norm"] = df.get("publisher_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["publisher_nome"].map(normalize_text)
    )
    df["dominio_url_principal"] = df.get("dominio_url_principal", pd.Series(pd.NA, index=df.index)).fillna(
        df["url_principal_atual"].map(extract_domain)
    )
    df["tem_doaj"] = safe_bool_series(df["tem_doaj"].fillna(False))
    df["tem_latindex"] = safe_bool_series(df["tem_latindex"].fillna(False))
    df["tem_titledb"] = safe_bool_series(df["tem_titledb"].fillna(True))
    return df


def prepare_openalex(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "journal_title" in df.columns and "journal_title_openalex" not in df.columns:
        rename_map["journal_title"] = "journal_title_openalex"
    if "issn" in df.columns and "issn_openalex" not in df.columns:
        rename_map["issn"] = "issn_openalex"
    if "eissn" in df.columns and "eissn_openalex" not in df.columns:
        rename_map["eissn"] = "eissn_openalex"
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in [
        "openalex_row_id", "source_id_openalex", "journal_title_openalex",
        "issn_openalex", "eissn_openalex", "publication_year", "article_count"
    ]:
        df = ensure_column(df, col)

    if df["openalex_row_id"].isna().all():
        df["openalex_row_id"] = [
            hash_id("OA_", [i, r.get("journal_title_openalex"), r.get("publication_year")])
            for i, r in df.iterrows()
        ]

    df["journal_title_norm"] = df.get("journal_title_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["journal_title_openalex"].map(normalize_text)
    )
    df["issn_openalex_norm"] = df.get("issn_openalex_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["issn_openalex"].map(normalize_issn)
    )
    df["eissn_openalex_norm"] = df.get("eissn_openalex_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["eissn_openalex"].map(normalize_issn)
    )
    df["publication_year"] = pd.to_numeric(df["publication_year"], errors="coerce").astype("Int64")
    df["article_count"] = pd.to_numeric(df["article_count"], errors="coerce").fillna(0).astype("Int64")
    return df


def prepare_doaj(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "titulo" in df.columns and "titulo_doaj" not in df.columns:
        rename_map["titulo"] = "titulo_doaj"
    if "issn" in df.columns and "issn_doaj" not in df.columns:
        rename_map["issn"] = "issn_doaj"
    if "eissn" in df.columns and "eissn_doaj" not in df.columns:
        rename_map["eissn"] = "eissn_doaj"
    if "publisher" in df.columns and "publisher_doaj" not in df.columns:
        rename_map["publisher"] = "publisher_doaj"
    if "url_journal" in df.columns and "url_journal_doaj" not in df.columns:
        rename_map["url_journal"] = "url_journal_doaj"
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ["doaj_id", "titulo_doaj", "issn_doaj", "eissn_doaj", "publisher_doaj", "url_journal_doaj"]:
        df = ensure_column(df, col)

    if df["doaj_id"].isna().all():
        df["doaj_id"] = [
            hash_id("DOAJ_", [i, r.get("titulo_doaj"), r.get("issn_doaj"), r.get("eissn_doaj")])
            for i, r in df.iterrows()
        ]

    df["titulo_norm"] = df.get("titulo_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["titulo_doaj"].map(normalize_text)
    )
    df["issn_doaj_norm"] = df.get("issn_doaj_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["issn_doaj"].map(normalize_issn)
    )
    df["eissn_doaj_norm"] = df.get("eissn_doaj_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["eissn_doaj"].map(normalize_issn)
    )
    df["publisher_norm"] = df.get("publisher_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["publisher_doaj"].map(normalize_text)
    )
    df["dominio_url_doaj"] = df.get("dominio_url_doaj", pd.Series(pd.NA, index=df.index)).fillna(
        df["url_journal_doaj"].map(extract_domain)
    )
    return df


def prepare_latindex(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    if "titulo" in df.columns and "titulo_latindex" not in df.columns:
        rename_map["titulo"] = "titulo_latindex"
    if "issn" in df.columns and "issn_latindex" not in df.columns:
        rename_map["issn"] = "issn_latindex"
    if "eissn" in df.columns and "eissn_latindex" not in df.columns:
        rename_map["eissn"] = "eissn_latindex"
    if "publisher" in df.columns and "publisher_latindex" not in df.columns:
        rename_map["publisher"] = "publisher_latindex"
    if "url_journal" in df.columns and "url_journal_latindex" not in df.columns:
        rename_map["url_journal"] = "url_journal_latindex"
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ["latindex_id", "titulo_latindex", "issn_latindex", "eissn_latindex", "publisher_latindex", "url_journal_latindex"]:
        df = ensure_column(df, col)

    if df["latindex_id"].isna().all():
        df["latindex_id"] = [
            hash_id("LAT_", [i, r.get("titulo_latindex"), r.get("issn_latindex"), r.get("eissn_latindex")])
            for i, r in df.iterrows()
        ]

    df["titulo_norm"] = df.get("titulo_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["titulo_latindex"].map(normalize_text)
    )
    df["issn_latindex_norm"] = df.get("issn_latindex_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["issn_latindex"].map(normalize_issn)
    )
    df["eissn_latindex_norm"] = df.get("eissn_latindex_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["eissn_latindex"].map(normalize_issn)
    )
    df["publisher_norm"] = df.get("publisher_norm", pd.Series(pd.NA, index=df.index)).fillna(
        df["publisher_latindex"].map(normalize_text)
    )
    df["dominio_url_latindex"] = df.get("dominio_url_latindex", pd.Series(pd.NA, index=df.index)).fillna(
        df["url_journal_latindex"].map(extract_domain)
    )
    return df


def prepare_infra(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["periodico_id", "redirect_count", "domain_changed", "is_accessible", "ojs_detected"]:
        df = ensure_column(df, col)
    df["redirect_count"] = pd.to_numeric(df["redirect_count"], errors="coerce").fillna(0).astype("Int64")
    df["domain_changed"] = safe_bool_series(df["domain_changed"])
    df["is_accessible"] = safe_bool_series(df["is_accessible"])
    df["ojs_detected"] = safe_bool_series(df["ojs_detected"])
    return df


def score_match(periodico: pd.Series, candidate: pd.Series, source: str) -> Tuple[float, str]:
    score = 0.0
    strategy = []

    p_issns = {periodico.get("issn_impresso_norm"), periodico.get("issn_eletronico_norm")}
    p_issns = {x for x in p_issns if x is not None and pd.notna(x)}

    if source == "openalex":
        c_issns = {candidate.get("issn_openalex_norm"), candidate.get("eissn_openalex_norm")}
        c_title = candidate.get("journal_title_norm")
        c_domain = None
    elif source == "doaj":
        c_issns = {candidate.get("issn_doaj_norm"), candidate.get("eissn_doaj_norm")}
        c_title = candidate.get("titulo_norm")
        c_domain = candidate.get("dominio_url_doaj")
    else:
        c_issns = {candidate.get("issn_latindex_norm"), candidate.get("eissn_latindex_norm")}
        c_title = candidate.get("titulo_norm")
        c_domain = candidate.get("dominio_url_latindex")

    c_issns = {x for x in c_issns if x is not None and pd.notna(x)}

    if p_issns.intersection(c_issns):
        score += 100
        strategy.append("issn")

    p_title = periodico.get("titulo_norm")
    if pd.notna(p_title) and pd.notna(c_title):
        if p_title == c_title:
            score += 40
            strategy.append("titulo_exato")
        elif str(p_title) in str(c_title) or str(c_title) in str(p_title):
            score += 20
            strategy.append("titulo_contido")

    p_pub = periodico.get("publisher_norm")
    c_pub = candidate.get("publisher_norm")
    if pd.notna(p_pub) and pd.notna(c_pub) and p_pub == c_pub:
        score += 15
        strategy.append("publisher")

    p_domain = periodico.get("dominio_url_principal")
    if pd.notna(p_domain) and pd.notna(c_domain) and p_domain == c_domain:
        score += 25
        strategy.append("dominio")

    return score, "+".join(strategy) if strategy else "sem_match"


def reconcile_source(periodicos: pd.DataFrame, source_df: pd.DataFrame, source_name: str, source_id_col: str, threshold: float):
    periodicos = periodicos.copy()
    matches = []

    source_by_issn = {}
    for idx, row in source_df.iterrows():
        if source_name == "openalex":
            issns = [row.get("issn_openalex_norm"), row.get("eissn_openalex_norm")]
        elif source_name == "doaj":
            issns = [row.get("issn_doaj_norm"), row.get("eissn_doaj_norm")]
        else:
            issns = [row.get("issn_latindex_norm"), row.get("eissn_latindex_norm")]

        for issn in issns:
            if issn is not None and pd.notna(issn):
                source_by_issn.setdefault(str(issn), []).append(idx)

    for p_idx, p_row in periodicos.iterrows():
        candidate_indices = set()

        for p_issn in [p_row.get("issn_impresso_norm"), p_row.get("issn_eletronico_norm")]:
            if p_issn is not None and pd.notna(p_issn):
                candidate_indices.update(source_by_issn.get(str(p_issn), []))

        if not candidate_indices:
            p_title = p_row.get("titulo_norm")
            if p_title:
                if source_name == "openalex":
                    title_hits = source_df.index[source_df["journal_title_norm"] == p_title].tolist()
                else:
                    title_hits = source_df.index[source_df["titulo_norm"] == p_title].tolist()
                candidate_indices.update(title_hits)

        best_score = -1.0
        best_idx = None
        best_strategy = "sem_match"

        for c_idx in candidate_indices:
            c_row = source_df.loc[c_idx]
            score, strategy = score_match(p_row, c_row, source_name)
            if score > best_score:
                best_score = score
                best_idx = c_idx
                best_strategy = strategy

        if best_idx is not None and best_score >= threshold:
            c_row = source_df.loc[best_idx]
            matches.append({
                "match_id": hash_id("MAT_", [p_row["periodico_id"], source_name, c_row[source_id_col]]),
                "periodico_id": p_row["periodico_id"],
                "source_table": source_name,
                "source_record_id": c_row[source_id_col],
                "match_strategy": best_strategy,
                "match_score": round(best_score, 2),
                "matched_by": "script",
                "reviewed_manually": False,
                "review_status": "auto_accepted",
                "review_notes": None,
                "created_at": pd.Timestamp.utcnow(),
            })
            if source_name == "doaj":
                periodicos.at[p_idx, "tem_doaj"] = True
            elif source_name == "latindex":
                periodicos.at[p_idx, "tem_latindex"] = True

    return periodicos, pd.DataFrame(matches)


def summarize_editorial(df_openalex: pd.DataFrame, periodicos: pd.DataFrame) -> pd.DataFrame:
    if df_openalex.empty:
        return pd.DataFrame(columns=[
            "periodico_id",
            "primeiro_ano_editorial",
            "ultimo_ano_editorial",
            "anos_editoriais_distintos",
            "total_artigos"
        ])

    df = df_openalex.copy()

    df_issn = pd.concat([
        df[["issn_openalex_norm", "publication_year", "article_count"]].rename(columns={"issn_openalex_norm": "issn"}),
        df[["eissn_openalex_norm", "publication_year", "article_count"]].rename(columns={"eissn_openalex_norm": "issn"})
    ], ignore_index=True)

    df_issn = df_issn[df_issn["issn"].notna()]

    p = periodicos.copy()
    p_issn = pd.concat([
        p[["periodico_id", "issn_impresso_norm"]].rename(columns={"issn_impresso_norm": "issn"}),
        p[["periodico_id", "issn_eletronico_norm"]].rename(columns={"issn_eletronico_norm": "issn"})
    ], ignore_index=True)

    p_issn = p_issn[p_issn["issn"].notna()]

    merged = p_issn.merge(df_issn, on="issn", how="left")

    return (
        merged.groupby("periodico_id", dropna=False)
        .agg(
            primeiro_ano_editorial=("publication_year", "min"),
            ultimo_ano_editorial=("publication_year", "max"),
            anos_editoriais_distintos=("publication_year", "nunique"),
            total_artigos=("article_count", "sum"),
        )
        .reset_index()
    )


def summarize_preservacao(df_titledb: pd.DataFrame) -> pd.DataFrame:
    if df_titledb.empty:
        return pd.DataFrame(columns=["periodico_id", "primeiro_ano_preservado", "ultimo_ano_preservado", "anos_preservados_distintos"])

    df = df_titledb.copy()
    df = ensure_column(df, "periodico_id")
    df = ensure_column(df, "year_declared")
    df["year_declared"] = pd.to_numeric(df["year_declared"], errors="coerce").astype("Int64")

    return (
        df.groupby("periodico_id", dropna=False)
        .agg(
            primeiro_ano_preservado=("year_declared", "min"),
            ultimo_ano_preservado=("year_declared", "max"),
            anos_preservados_distintos=("year_declared", "nunique"),
        )
        .reset_index()
    )


def summarize_infra(df_infra: pd.DataFrame) -> pd.DataFrame:
    if df_infra.empty:
        return pd.DataFrame(columns=["periodico_id", "url_titledb_acessivel", "url_com_redirect", "dominio_alterado"])

    return (
        df_infra.groupby("periodico_id", dropna=False)
        .agg(
            url_titledb_acessivel=("is_accessible", lambda s: s.fillna(False).astype("boolean").any()),
            url_com_redirect=("redirect_count", lambda s: (s.fillna(0).astype("Int64") > 0).any()),
            dominio_alterado=("domain_changed", lambda s: s.fillna(False).astype("boolean").any()),
        )
        .reset_index()
    )


def classify_categoria(primeiro, ultimo, anos):
    if pd.isna(primeiro) or pd.isna(ultimo) or pd.isna(anos):
        return pd.NA
    if int(primeiro) >= 2018:
        return "entrante"
    if int(ultimo) <= 2019:
        return "retirante"
    if int(anos) >= 8 and int(ultimo) >= 2024:
        return "continuante"
    return "transiente"


def classify_risco(row) -> str:
    ultimo_editorial = row.get("ultimo_ano_editorial")
    ultimo_preservado = row.get("ultimo_ano_preservado")
    gap = row.get("gap_anos_editorial_preservacao")
    url_ok = row.get("url_titledb_acessivel")
    dom_changed = row.get("dominio_alterado")

    tem_doaj = row.get("tem_doaj")
    tem_latindex = row.get("tem_latindex")

    tem_dados = pd.notna(ultimo_editorial)
    ativo = tem_dados and int(ultimo_editorial) >= 2024

    indexado = (
        (pd.notna(tem_doaj) and bool(tem_doaj)) or
        (pd.notna(tem_latindex) and bool(tem_latindex))
    )

    sem_preservacao = pd.isna(ultimo_preservado)
    gap_ge_3 = pd.notna(gap) and int(gap) >= 3
    gap_ge_1 = pd.notna(gap) and int(gap) >= 1

    if not tem_dados:
        if indexado and (sem_preservacao or gap_ge_3):
            return "alto"
        return "baixo"

    if not ativo:
        if indexado and (sem_preservacao or gap_ge_3):
            return "alto"
        return "baixo"

    if sem_preservacao:
        return "critico" if indexado else "alto"

    if gap_ge_3:
        if url_ok is False or dom_changed is True:
            return "critico"
        return "alto"

    if gap_ge_1:
        return "medio"

    return "baixo"


def motivo_risco(row) -> str:
    motivos = []
    tem_dados_editoriais = row.get("tem_dados_editoriais")

    if pd.notna(tem_dados_editoriais) and (not bool(tem_dados_editoriais)):
        motivos.append("sem dados editoriais observaveis")
    if row.get("desalinhamento_editorial_preservacao"):
        motivos.append("ativo editorialmente sem preservacao recente")
    if row.get("desalinhamento_indexacao_preservacao"):
        motivos.append("indexado sem preservacao recente")
    if row.get("url_titledb_acessivel") is False:
        motivos.append("url inacessivel")
    if row.get("dominio_alterado") is True:
        motivos.append("dominio alterado")
    if row.get("url_com_redirect") is True:
        motivos.append("redirect detectado")
    return "; ".join(motivos) if motivos else "sem sinal critico relevante"

def mark_index_presence_by_issn(periodicos: pd.DataFrame, source_df: pd.DataFrame, source: str) -> pd.DataFrame:
    periodicos = periodicos.copy()

    if source == "latindex":
        source_issns = set(source_df["issn_latindex_norm"].dropna()) | set(source_df["eissn_latindex_norm"].dropna())
        target_col = "tem_latindex"
    elif source == "doaj":
        source_issns = set(source_df["issn_doaj_norm"].dropna()) | set(source_df["eissn_doaj_norm"].dropna())
        target_col = "tem_doaj"
    else:
        raise ValueError("source deve ser 'latindex' ou 'doaj'")

    periodicos[target_col] = (
        periodicos["issn_impresso_norm"].isin(source_issns) |
        periodicos["issn_eletronico_norm"].isin(source_issns)
    )

    periodicos[target_col] = periodicos[target_col].astype("boolean")
    return periodicos

def main():
    parser = argparse.ArgumentParser(description="Reconciliador Cariniana")
    parser.add_argument("--periodicos", type=Path, required=True)
    parser.add_argument("--openalex", type=Path)
    parser.add_argument("--doaj", type=Path)
    parser.add_argument("--latindex", type=Path)
    parser.add_argument("--infra", type=Path)
    parser.add_argument("--titledb", type=Path)
    parser.add_argument("--outdir", type=Path, default=Path("."))
    args = parser.parse_args()

    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    periodicos = prepare_periodicos(load_csv(args.periodicos))
    auditorias = []

    if args.openalex and args.openalex.exists():
        openalex = prepare_openalex(load_csv(args.openalex))
        periodicos, aud = reconcile_source(periodicos, openalex, "openalex", "openalex_row_id", threshold=80.0)
        auditorias.append(aud)
    else:
        openalex = pd.DataFrame()

    if args.doaj and args.doaj.exists():
        doaj = prepare_doaj(load_csv(args.doaj))
        periodicos = mark_index_presence_by_issn(periodicos, doaj, "doaj")
        periodicos, aud = reconcile_source(periodicos, doaj, "doaj", "doaj_id", threshold=90.0)
        auditorias.append(aud)
    else:
        doaj = pd.DataFrame()

    if args.latindex and args.latindex.exists():
        latindex = prepare_latindex(load_csv(args.latindex))
        periodicos = mark_index_presence_by_issn(periodicos, latindex, "latindex")
        periodicos, aud = reconcile_source(periodicos, latindex, "latindex", "latindex_id", threshold=90.0)
        auditorias.append(aud)
    else:
        latindex = pd.DataFrame()

    auditoria_match = pd.concat(auditorias, ignore_index=True) if auditorias else pd.DataFrame(
        columns=[
            "match_id", "periodico_id", "source_table", "source_record_id", "match_strategy",
            "match_score", "matched_by", "reviewed_manually", "review_status", "review_notes", "created_at"
        ]
    )

    editorial_summary = summarize_editorial(openalex, periodicos)
    preservacao_summary = summarize_preservacao(load_csv(args.titledb)) if args.titledb and args.titledb.exists() else pd.DataFrame(
        columns=["periodico_id", "primeiro_ano_preservado", "ultimo_ano_preservado", "anos_preservados_distintos"]
    )
    infra_summary = summarize_infra(prepare_infra(load_csv(args.infra))) if args.infra and args.infra.exists() else pd.DataFrame(
        columns=["periodico_id", "url_titledb_acessivel", "url_com_redirect", "dominio_alterado"]
    )

    periodicos_enriquecido = periodicos.merge(editorial_summary, on="periodico_id", how="left")
    periodicos_enriquecido = periodicos_enriquecido.merge(preservacao_summary, on="periodico_id", how="left")
    periodicos_enriquecido = periodicos_enriquecido.merge(infra_summary, on="periodico_id", how="left")
    periodicos_enriquecido["ativo_editorial_recente"] = (
        pd.to_numeric(periodicos_enriquecido["ultimo_ano_editorial"], errors="coerce").fillna(-1) >= 2024
    )

    diag = periodicos_enriquecido[[
        "periodico_id", "titulo_principal", "issn_impresso", "issn_eletronico",
        "tem_doaj", "tem_latindex", "tem_titledb",
        "primeiro_ano_editorial", "ultimo_ano_editorial", "anos_editoriais_distintos",
        "primeiro_ano_preservado", "ultimo_ano_preservado", "anos_preservados_distintos",
        "url_titledb_acessivel", "url_com_redirect", "dominio_alterado"
    ]].copy()

    for col in [
        "primeiro_ano_editorial", "ultimo_ano_editorial", "anos_editoriais_distintos",
        "primeiro_ano_preservado", "ultimo_ano_preservado", "anos_preservados_distintos"
    ]:
        diag[col] = pd.to_numeric(diag[col], errors="coerce").astype("Int64")

    diag["gap_anos_editorial_preservacao"] = (
        diag["ultimo_ano_editorial"] - diag["ultimo_ano_preservado"]
    ).astype("Int64")

    diag["categoria_editorial"] = diag.apply(
        lambda r: classify_categoria(r["primeiro_ano_editorial"], r["ultimo_ano_editorial"], r["anos_editoriais_distintos"]),
        axis=1
    )
    diag["categoria_preservacao"] = diag.apply(
        lambda r: classify_categoria(r["primeiro_ano_preservado"], r["ultimo_ano_preservado"], r["anos_preservados_distintos"]),
        axis=1
    )

    diag["desalinhamento_editorial_preservacao"] = (
        (diag["ultimo_ano_editorial"].fillna(-1) >= 2024) &
        (diag["ultimo_ano_preservado"].isna() | (diag["ultimo_ano_preservado"].fillna(-1) <= 2023))
    )

    diag["desalinhamento_indexacao_preservacao"] = (
        (diag["tem_doaj"].fillna(False).astype(bool) | diag["tem_latindex"].fillna(False).astype(bool)) &
        (diag["ultimo_ano_preservado"].isna() | (diag["ultimo_ano_preservado"].fillna(-1) <= 2023))
    )

    diag["tem_dados_editoriais"] = diag["anos_editoriais_distintos"].fillna(0) > 0

    tem_doaj = diag["tem_doaj"].fillna(False).astype(bool)
    tem_latindex = diag["tem_latindex"].fillna(False).astype(bool)
    indexado = tem_doaj | tem_latindex
    ultimo_preservado = pd.to_numeric(diag["ultimo_ano_preservado"], errors="coerce")

    diag["risco_estrutural"] = (
        indexado &
        (ultimo_preservado.isna() | (ultimo_preservado <= 2023))
    )

    diag["usa_automacao_plugin"] = pd.NA
    diag["grau_risco"] = diag.apply(classify_risco, axis=1)
    diag["motivo_risco"] = diag.apply(motivo_risco, axis=1)
    diag["observacao_analitica"] = pd.NA
    diag["snapshot_date"] = pd.Timestamp.utcnow().date()
    diag["diagnostico_id"] = [
        hash_id("DIAG_", [pid, snap]) for pid, snap in zip(diag["periodico_id"], diag["snapshot_date"])
    ]
    diag["created_at"] = pd.Timestamp.utcnow()

    periodicos_enriquecido.to_csv(outdir / "periodicos_enriquecido.csv", index=False, encoding="utf-8")
    diag.to_csv(outdir / "diagnostico_risco.csv", index=False, encoding="utf-8")
    auditoria_match.to_csv(outdir / "auditoria_match.csv", index=False, encoding="utf-8")

    print(f"Periodicos: {len(periodicos_enriquecido)}")
    print(f"Diagnosticos: {len(diag)}")
    print(f"Auditorias de match: {len(auditoria_match)}")
    print(f"Arquivos gravados em: {outdir.resolve()}")


if __name__ == "__main__":
    main()
