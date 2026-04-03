"""
preparar_bases_reais_cariniana.py

Prepara as três bases reais informadas pelo usuário para o pipeline Cariniana.

Entradas brutas:
- openalex_BR_2000_2024_tratado.csv
- doaj_brasil_extraido_do_json.csv
- latindex-journals-brasileiros.csv

Saídas:
- editorial_openalex.csv
- indexacao_doaj.csv
- indexacao_latindex.csv
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional, Tuple
from urllib.parse import urlparse

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


def extract_domain(url):
    if pd.isna(url) or url is None or not str(url).strip():
        return None
    try:
        return urlparse(str(url).strip()).netloc.lower() or None
    except Exception:
        return None


def parse_issn_list(raw_value) -> list[str]:
    if pd.isna(raw_value) or raw_value is None:
        return []
    text = str(raw_value).strip()
    if not text or text == "[]":
        return []
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            out = []
            for item in parsed:
                norm = normalize_issn(item)
                if norm:
                    out.append(norm)
            seen = set()
            uniq = []
            for x in out:
                if x not in seen:
                    uniq.append(x)
                    seen.add(x)
            return uniq
    except Exception:
        pass
    candidates = re.findall(r"[0-9Xx]{4}-?[0-9Xx]{4}", text)
    out = []
    seen = set()
    for c in candidates:
        norm = normalize_issn(c)
        if norm and norm not in seen:
            out.append(norm)
            seen.add(norm)
    return out


def split_primary_secondary_issn(row) -> Tuple[Optional[str], Optional[str]]:
    issn_l = normalize_issn(row.get("issn_l"))
    issns = parse_issn_list(row.get("issn"))
    if not issns and issn_l:
        return issn_l, None
    if issn_l and issn_l in issns:
        other = [x for x in issns if x != issn_l]
        return issn_l, (other[0] if other else None)
    if len(issns) >= 2:
        return issns[0], issns[1]
    if len(issns) == 1:
        return issns[0], None
    return None, None


def preparar_openalex(openalex_path: Path) -> pd.DataFrame:
    df = pd.read_csv(openalex_path, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    required = {"journal_title", "ano", "issn_l", "issn"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"OpenAlex bruto sem colunas esperadas: {sorted(missing)}")

    issn_pairs = df.apply(split_primary_secondary_issn, axis=1, result_type="expand")
    issn_pairs.columns = ["issn_openalex", "eissn_openalex"]
    df = pd.concat([df, issn_pairs], axis=1)

    df["journal_title_openalex"] = df["journal_title"]
    df["journal_title_norm"] = df["journal_title_openalex"].map(normalize_text)
    df["issn_openalex"] = df["issn_openalex"].map(normalize_issn)
    df["eissn_openalex"] = df["eissn_openalex"].map(normalize_issn)
    df["issn_openalex_norm"] = df["issn_openalex"]
    df["eissn_openalex_norm"] = df["eissn_openalex"]
    df["issn_l"] = df["issn_l"].map(normalize_issn)
    df["publication_year"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    grouped = (
        df.groupby(
            [
                "journal_title_openalex",
                "journal_title_norm",
                "issn_openalex",
                "eissn_openalex",
                "issn_openalex_norm",
                "eissn_openalex_norm",
                "issn_l",
                "publication_year",
            ],
            dropna=False,
        )
        .agg(
            article_count=("titulo", "count"),
            br_affiliation_count_sum=("br_affiliation_count", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
            authors_count_sum=("authors_count", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
            has_br_affiliation_any=("has_br_affiliation", lambda s: s.astype("string").str.lower().isin(["true", "1"]).any()),
            has_br_author_country_any=("has_br_author_country", lambda s: s.astype("string").str.lower().isin(["true", "1"]).any()),
        )
        .reset_index()
    )

    grouped["source_id_openalex"] = grouped.apply(
        lambda r: hash_id("SRC_", [r["journal_title_openalex"], r["issn_openalex"], r["eissn_openalex"], r["issn_l"]]),
        axis=1,
    )
    grouped["openalex_row_id"] = grouped.apply(
        lambda r: hash_id("OA_", [r["source_id_openalex"], r["publication_year"]]),
        axis=1,
    )
    grouped["oa_status"] = pd.NA
    grouped["country_inferred"] = "BR"
    grouped["is_brazilian_scope"] = True
    grouped["data_source_file"] = openalex_path.name
    grouped["created_at"] = pd.Timestamp.utcnow()

    cols = [
        "openalex_row_id",
        "source_id_openalex",
        "journal_title_openalex",
        "journal_title_norm",
        "issn_openalex",
        "eissn_openalex",
        "issn_openalex_norm",
        "eissn_openalex_norm",
        "issn_l",
        "publication_year",
        "article_count",
        "br_affiliation_count_sum",
        "authors_count_sum",
        "has_br_affiliation_any",
        "has_br_author_country_any",
        "oa_status",
        "country_inferred",
        "is_brazilian_scope",
        "data_source_file",
        "created_at",
    ]
    return grouped[cols].sort_values(["journal_title_openalex", "publication_year"]).reset_index(drop=True)


def preparar_doaj(doaj_path: Path) -> pd.DataFrame:
    df = pd.read_csv(doaj_path, dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    required = {"id", "title", "journal_url", "eissn", "pissn", "publisher_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DOAJ bruto sem colunas esperadas: {sorted(missing)}")

    out = pd.DataFrame()
    out["doaj_id"] = df["id"]
    out["periodico_id"] = pd.NA
    out["titulo_doaj"] = df["title"]
    out["titulo_norm"] = out["titulo_doaj"].map(normalize_text)
    out["issn_doaj"] = df["pissn"].map(normalize_issn)
    out["eissn_doaj"] = df["eissn"].map(normalize_issn)
    out["issn_doaj_norm"] = out["issn_doaj"]
    out["eissn_doaj_norm"] = out["eissn_doaj"]
    out["publisher_doaj"] = df["publisher_name"]
    out["publisher_norm"] = out["publisher_doaj"].map(normalize_text)
    out["country_doaj"] = df.get("publisher_country", pd.Series(pd.NA, index=df.index))
    out["url_journal_doaj"] = df["journal_url"]
    out["dominio_url_doaj"] = out["url_journal_doaj"].map(extract_domain)
    out["url_oa_start"] = pd.NA
    out["subjects_doaj"] = pd.NA
    out["license_doaj"] = pd.NA
    out["seal_doaj"] = pd.NA
    out["apc_doaj"] = pd.NA
    out["added_on_doaj"] = pd.to_datetime(df.get("oa_start_year", pd.Series(pd.NA, index=df.index)), format="%Y", errors="coerce")
    out["last_updated_doaj"] = pd.NaT
    out["in_doaj_current"] = True
    out["created_at"] = pd.Timestamp.utcnow()

    cols = [
        "doaj_id","periodico_id","titulo_doaj","titulo_norm","issn_doaj","eissn_doaj",
        "issn_doaj_norm","eissn_doaj_norm","publisher_doaj","publisher_norm",
        "country_doaj","url_journal_doaj","dominio_url_doaj","url_oa_start",
        "subjects_doaj","license_doaj","seal_doaj","apc_doaj","added_on_doaj",
        "last_updated_doaj","in_doaj_current","created_at",
    ]
    return out[cols].sort_values("titulo_doaj").reset_index(drop=True)


def preparar_latindex(latindex_path: Path) -> pd.DataFrame:
    df = pd.read_csv(latindex_path, sep=";", dtype="string", keep_default_na=True, na_values=["", "NA", "None", "null"])
    required = {"folio_u", "tit_propio", "nombre_edi", "issn_e", "issn_imp"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Latindex bruto sem colunas esperadas: {sorted(missing)}")

    out = pd.DataFrame()
    out["latindex_id"] = df["folio_u"].astype("string")
    out["periodico_id"] = pd.NA
    out["titulo_latindex"] = df["tit_propio"]
    out["titulo_norm"] = out["titulo_latindex"].map(normalize_text)
    out["issn_latindex"] = df["issn_imp"].map(normalize_issn)
    out["eissn_latindex"] = df["issn_e"].map(normalize_issn)
    out["issn_latindex_norm"] = out["issn_latindex"]
    out["eissn_latindex_norm"] = out["eissn_latindex"]
    out["publisher_latindex"] = df["nombre_edi"]
    out["publisher_norm"] = out["publisher_latindex"].map(normalize_text)
    out["country_latindex"] = df.get("nombre_largo", pd.Series(pd.NA, index=df.index))
    out["url_journal_latindex"] = pd.NA
    out["dominio_url_latindex"] = pd.NA
    out["status_latindex"] = df.get("catalogada", pd.Series(pd.NA, index=df.index)).map(
        lambda x: "catalogada" if str(x).strip() == "1" else ("diretorio" if pd.notna(x) else pd.NA)
    )
    out["area_latindex"] = df.get("subtemas", pd.Series(pd.NA, index=df.index))
    out["in_latindex_current"] = True
    out["last_checked_latindex"] = pd.Timestamp.utcnow().normalize()
    out["created_at"] = pd.Timestamp.utcnow()

    cols = [
        "latindex_id","periodico_id","titulo_latindex","titulo_norm","issn_latindex",
        "eissn_latindex","issn_latindex_norm","eissn_latindex_norm","publisher_latindex",
        "publisher_norm","country_latindex","url_journal_latindex","dominio_url_latindex",
        "status_latindex","area_latindex","in_latindex_current","last_checked_latindex","created_at",
    ]
    return out[cols].sort_values("titulo_latindex").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Preparador das bases reais para o pipeline Cariniana")
    parser.add_argument("--openalex-bruto", type=Path, required=True)
    parser.add_argument("--doaj-bruto", type=Path, required=True)
    parser.add_argument("--latindex-bruto", type=Path, required=True)
    parser.add_argument("--outdir", type=Path, default=Path("."))
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)

    editorial_openalex = preparar_openalex(args.openalex_bruto)
    indexacao_doaj = preparar_doaj(args.doaj_bruto)
    indexacao_latindex = preparar_latindex(args.latindex_bruto)

    editorial_openalex.to_csv(args.outdir / "editorial_openalex.csv", index=False, encoding="utf-8")
    indexacao_doaj.to_csv(args.outdir / "indexacao_doaj.csv", index=False, encoding="utf-8")
    indexacao_latindex.to_csv(args.outdir / "indexacao_latindex.csv", index=False, encoding="utf-8")

    print(f"editorial_openalex.csv: {len(editorial_openalex)} linhas")
    print(f"indexacao_doaj.csv: {len(indexacao_doaj)} linhas")
    print(f"indexacao_latindex.csv: {len(indexacao_latindex)} linhas")
    print(f"Arquivos gravados em: {args.outdir.resolve()}")


if __name__ == "__main__":
    main()
