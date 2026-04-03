"""
parser_titledb_cariniana.py

Extrai entradas de titledb.xml do LOCKSS/Cariniana e gera:
1. preservacao_titledb.csv
2. periodicos_base.csv

Uso:
    python parser_titledb_cariniana.py /caminho/titledb.xml --outdir ./saida

Observações:
- O parser assume a estrutura padrão de property/property usada em titledb.xml.
- O consolidado de periodicos é apenas uma base inicial; a reconciliação final ainda deve
  cruzar com OpenAlex, DOAJ e Latindex.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = html.unescape(str(value)).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"\s+", " ", value)
    return value or None


def normalize_issn(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip().upper().replace(" ", "").replace("-", "")
    if len(value) != 8:
        return None
    return f"{value[:4]}-{value[4:]}"


def extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        return urlparse(url).netloc.lower() or None
    except Exception:
        return None


def safe_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return html.unescape(str(value)).strip() or None


def hash_id(prefix: str, parts: Iterable[Optional[str]], size: int = 16) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:size]
    return f"{prefix}{digest.upper()}"


def child_properties(element: ET.Element) -> List[ET.Element]:
    return [child for child in list(element) if child.tag == "property"]


def property_name(element: ET.Element) -> Optional[str]:
    return element.attrib.get("name")


def property_value(element: ET.Element) -> Optional[str]:
    return safe_text(element.attrib.get("value"))


def parse_nested_param(param_element: ET.Element) -> Dict[str, Optional[str]]:
    result = {"key": None, "value": None}
    for child in child_properties(param_element):
        name = property_name(child)
        if name == "key":
            result["key"] = property_value(child)
        elif name == "value":
            result["value"] = property_value(child)
    return result


def parse_top_entry(entry_element: ET.Element, xml_source_file: str) -> Dict[str, Optional[str]]:
    """
    Parse de uma entrada top-level do titledb:
    <property name="OJS3PluginRBCIAMB9_2008"> ... </property>
    """
    data: Dict[str, Optional[str]] = {
        "plugin_identifier": property_name(entry_element),
        "plugin_name": None,
        "journal_title_titledb": None,
        "journal_title_norm": None,
        "title_entry": None,
        "publisher_titledb": None,
        "publisher_norm": None,
        "issn_titledb": None,
        "eissn_titledb": None,
        "issn_titledb_norm": None,
        "eissn_titledb_norm": None,
        "base_url": None,
        "base_url_domain": None,
        "journal_id": None,
        "year_declared": None,
        "volume_declared": None,
        "type_declared": None,
        "attributes_year": None,
        "attributes_volume": None,
        "xml_source_file": xml_source_file,
    }

    for child in child_properties(entry_element):
        name = property_name(child)

        if name == "attributes.publisher":
            data["publisher_titledb"] = property_value(child)
            data["publisher_norm"] = normalize_text(data["publisher_titledb"])
        elif name == "journalTitle":
            data["journal_title_titledb"] = property_value(child)
            data["journal_title_norm"] = normalize_text(data["journal_title_titledb"])
        elif name == "issn":
            data["issn_titledb"] = property_value(child)
            data["issn_titledb_norm"] = normalize_issn(data["issn_titledb"])
        elif name == "eissn":
            data["eissn_titledb"] = property_value(child)
            data["eissn_titledb_norm"] = normalize_issn(data["eissn_titledb"])
        elif name == "type":
            data["type_declared"] = property_value(child)
        elif name == "title":
            data["title_entry"] = property_value(child)
        elif name == "plugin":
            data["plugin_name"] = property_value(child)
        elif name == "attributes.year":
            value = property_value(child)
            data["attributes_year"] = int(value) if value and value.isdigit() else value
        elif name == "attributes.volume":
            data["attributes_volume"] = property_value(child)
        elif name and name.startswith("param."):
            param = parse_nested_param(child)
            if param["key"] == "base_url":
                data["base_url"] = param["value"]
                data["base_url_domain"] = extract_domain(data["base_url"])
            elif param["key"] == "journal_id":
                data["journal_id"] = param["value"]
            elif param["key"] == "year":
                value = param["value"]
                data["year_declared"] = int(value) if value and str(value).isdigit() else value
            elif param["key"] == "volume":
                data["volume_declared"] = param["value"]

    return data


def iter_top_entries(root: ET.Element) -> Iterable[ET.Element]:
    """
    Procura entradas top-level relevantes.
    O formato mais comum é:
    <map>
      <property name="OJS3PluginXYZ_2020">...</property>
    </map>
    """
    for elem in root.iter():
        if elem.tag != "property":
            continue
        name = property_name(elem)
        if not name:
            continue

        # Entradas top-level de AU normalmente possuem filhos.
        if child_properties(elem):
            # Heurística: precisa ter algum filho com campos típicos
            child_names = {property_name(c) for c in child_properties(elem)}
            if {"journalTitle", "plugin"} & child_names:
                yield elem


def build_periodicos_base(rows: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    """
    Consolida entradas de preservacao_titledb por periódico.
    Chave de agrupamento:
    1. ISSN/eISSN normalizados, se existirem
    2. fallback para titulo_norm + publisher_norm
    """
    groups: Dict[str, List[Dict[str, Optional[str]]]] = defaultdict(list)

    for row in rows:
        issn_key = row.get("issn_titledb_norm")
        eissn_key = row.get("eissn_titledb_norm")
        title_key = row.get("journal_title_norm")
        publisher_key = row.get("publisher_norm")

        if issn_key or eissn_key:
            group_key = f"ISSN::{issn_key or ''}::{eissn_key or ''}"
        else:
            group_key = f"TXT::{title_key or ''}::{publisher_key or ''}"

        groups[group_key].append(row)

    periodicos = []

    for group_key, entries in groups.items():
        entries_sorted = sorted(
            entries,
            key=lambda x: (
                x.get("journal_title_titledb") or "",
                x.get("publisher_titledb") or "",
                x.get("year_declared") if isinstance(x.get("year_declared"), int) else -9999,
            )
        )
        first = entries_sorted[0]

        anos = sorted(
            {
                e["year_declared"]
                for e in entries_sorted
                if isinstance(e.get("year_declared"), int)
            }
        )
        urls = [e.get("base_url") for e in entries_sorted if e.get("base_url")]
        domains = [e.get("base_url_domain") for e in entries_sorted if e.get("base_url_domain")]

        url_principal = None
        if urls:
            # Mais recente primeiro, se tiver ano
            entries_by_year = sorted(
                [e for e in entries_sorted if e.get("base_url")],
                key=lambda x: x.get("year_declared") if isinstance(x.get("year_declared"), int) else -9999,
                reverse=True
            )
            url_principal = entries_by_year[0].get("base_url")

        periodico_id = hash_id(
            "PER_",
            [
                first.get("issn_titledb_norm"),
                first.get("eissn_titledb_norm"),
                first.get("journal_title_norm"),
                first.get("publisher_norm"),
            ],
        )

        periodicos.append(
            {
                "periodico_id": periodico_id,
                "titulo_principal": first.get("journal_title_titledb"),
                "titulo_abreviado": None,
                "titulo_norm": first.get("journal_title_norm"),
                "issn_impresso": first.get("issn_titledb"),
                "issn_eletronico": first.get("eissn_titledb"),
                "issn_l": None,
                "issn_impresso_norm": first.get("issn_titledb_norm"),
                "issn_eletronico_norm": first.get("eissn_titledb_norm"),
                "issn_l_norm": None,
                "publisher_nome": first.get("publisher_titledb"),
                "publisher_norm": first.get("publisher_norm"),
                "pais": "Brasil",
                "area_tematica": None,
                "plataforma_editorial": infer_platform(first.get("plugin_name")),
                "url_principal_atual": url_principal,
                "dominio_url_principal": extract_domain(url_principal),
                "fonte_url_principal": "titledb",
                "ativo_editorial_recente": None,
                "ano_ultimo_registro_editorial": None,
                "tem_doaj": False,
                "tem_latindex": False,
                "tem_titledb": True,
                "observacao_match": f"gerado a partir do titledb; entradas={len(entries_sorted)}; anos={anos[0] if anos else ''}-{anos[-1] if anos else ''}",
            }
        )

        for entry in entries_sorted:
            entry["periodico_id"] = periodico_id

    return periodicos


def infer_platform(plugin_name: Optional[str]) -> Optional[str]:
    if not plugin_name:
        return None
    name = plugin_name.lower()
    if "ojs3" in name:
        return "OJS3"
    if "ojs2" in name:
        return "OJS2"
    if "ojs" in name:
        return "OJS"
    return None


def parse_titledb(xml_path: Path) -> List[Dict[str, Optional[str]]]:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows: List[Dict[str, Optional[str]]] = []

    for entry in iter_top_entries(root):
        parsed = parse_top_entry(entry, xml_source_file=xml_path.name)

        titledb_entry_id = hash_id(
            "TDB_",
            [
                parsed.get("plugin_identifier"),
                parsed.get("journal_title_titledb"),
                parsed.get("issn_titledb_norm"),
                parsed.get("eissn_titledb_norm"),
                parsed.get("year_declared"),
                parsed.get("volume_declared"),
                parsed.get("base_url"),
            ],
        )
        parsed["titledb_entry_id"] = titledb_entry_id
        parsed["xml_capture_date"] = None
        parsed["registro_ativo"] = True
        parsed["created_at"] = None
        parsed["periodico_id"] = None

        rows.append(parsed)

    return rows


def write_csv(path: Path, rows: List[Dict[str, Optional[str]]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extrai dados do titledb.xml da Cariniana/LOCKSS")
    parser.add_argument("xml_path", type=Path, help="Caminho para o arquivo titledb.xml")
    parser.add_argument("--outdir", type=Path, default=Path("."), help="Diretório de saída")
    args = parser.parse_args()

    xml_path: Path = args.xml_path
    outdir: Path = args.outdir

    outdir.mkdir(parents=True, exist_ok=True)

    rows = parse_titledb(xml_path)
    periodicos = build_periodicos_base(rows)

    titledb_fields = [
        "titledb_entry_id",
        "periodico_id",
        "plugin_name",
        "plugin_identifier",
        "journal_title_titledb",
        "journal_title_norm",
        "title_entry",
        "publisher_titledb",
        "publisher_norm",
        "issn_titledb",
        "eissn_titledb",
        "issn_titledb_norm",
        "eissn_titledb_norm",
        "base_url",
        "base_url_domain",
        "journal_id",
        "year_declared",
        "volume_declared",
        "type_declared",
        "attributes_year",
        "attributes_volume",
        "xml_source_file",
        "xml_capture_date",
        "registro_ativo",
        "created_at",
    ]

    periodicos_fields = [
        "periodico_id",
        "titulo_principal",
        "titulo_abreviado",
        "titulo_norm",
        "issn_impresso",
        "issn_eletronico",
        "issn_l",
        "issn_impresso_norm",
        "issn_eletronico_norm",
        "issn_l_norm",
        "publisher_nome",
        "publisher_norm",
        "pais",
        "area_tematica",
        "plataforma_editorial",
        "url_principal_atual",
        "dominio_url_principal",
        "fonte_url_principal",
        "ativo_editorial_recente",
        "ano_ultimo_registro_editorial",
        "tem_doaj",
        "tem_latindex",
        "tem_titledb",
        "observacao_match",
    ]

    write_csv(outdir / "preservacao_titledb.csv", rows, titledb_fields)
    write_csv(outdir / "periodicos_base.csv", periodicos, periodicos_fields)

    print(f"Entradas preservacao_titledb: {len(rows)}")
    print(f"Periodicos consolidados: {len(periodicos)}")
    print(f"Arquivos gerados em: {outdir.resolve()}")


if __name__ == "__main__":
    main()
