"""
validador_urls_cariniana.py

Lê um CSV de periódicos ou preservação_titledb e testa URLs para alimentar
a tabela/CSV `infraestrutura_url`.

Entradas aceitas:
1. periodicos_base.csv      -> usa `periodico_id` + `url_principal_atual`
2. preservacao_titledb.csv  -> usa `periodico_id` + `base_url`

Saídas:
- infraestrutura_url.csv
- resumo_validacao_urls.csv

Uso:
    python validador_urls_cariniana.py ./periodicos_base.csv --outdir ./saida
    python validador_urls_cariniana.py ./preservacao_titledb.csv --source-system titledb --outdir ./saida

Observações:
- Requer requests.
- O detector de OJS é heurístico.
- O script não resolve anti-bot, WAF ou bloqueios sofisticados.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests


DEFAULT_TIMEOUT = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36 CarinianaURLValidator/1.0"
)


def hash_id(prefix: str, parts: Iterable[Optional[str]], size: int = 16) -> str:
    raw = "||".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:size]
    return f"{prefix}{digest.upper()}"


def extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        return urlparse(str(url).strip()).netloc.lower() or None
    except Exception:
        return None


def normalize_bool(value) -> Optional[bool]:
    if value in (True, False):
        return value
    if value is None:
        return None
    txt = str(value).strip().lower()
    if txt in {"1", "true", "sim", "yes"}:
        return True
    if txt in {"0", "false", "nao", "não", "no"}:
        return False
    return None


def guess_ojs(html_text: str, headers: Dict[str, str]) -> tuple[bool, Optional[str]]:
    """
    Heurística simples para detectar OJS/OJS3.
    """
    haystack = (html_text or "")[:200000].lower()
    header_str = " ".join(f"{k}: {v}" for k, v in headers.items()).lower()

    patterns = [
        r"open journal systems",
        r"pkp-lib",
        r"ojs\.pkp",
        r"/index\.php/",
        r"powered by ojs",
        r"powered by open journal systems",
        r"pkp\s*\|",
    ]
    detected = any(re.search(p, haystack) for p in patterns) or "pkp" in header_str

    version = None
    version_patterns = [
        r"open journal systems\s*([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        r"ojs\s*([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        r"pkp-lib[^0-9]*([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
    ]
    for pattern in version_patterns:
        m = re.search(pattern, haystack)
        if m:
            version = m.group(1)
            break

    # Sinal fraco de OJS3
    if detected and version is None:
        if "pkp-lib" in haystack or "/api/v1/" in haystack or 'name="generator"' in haystack:
            version = "provavel-ojs3"

    return detected, version


def classify_problem(
    status_code: Optional[int],
    timeout: bool,
    exception_name: Optional[str],
    is_accessible: Optional[bool],
    domain_changed: Optional[bool],
    redirect_count: int,
) -> Optional[str]:
    if timeout:
        return "timeout"
    if exception_name:
        if "ssl" in exception_name.lower():
            return "ssl_error"
        if "connection" in exception_name.lower():
            return "connection_error"
        return "request_error"

    if status_code is None:
        return "unknown"

    if status_code in {401, 403}:
        return "blocked"
    if status_code == 404:
        return "not_found"
    if 500 <= status_code <= 599:
        return "server_error"
    if 300 <= status_code <= 399:
        return "redirect_only"
    if is_accessible and domain_changed:
        return "redirect_domain_changed"
    if is_accessible and redirect_count > 0:
        return "redirect"
    if is_accessible:
        return "ok"
    return "inaccessible"


def choose_input_columns(fieldnames: List[str]) -> tuple[str, str, str]:
    """
    Detecta de forma simples qual arquivo foi informado.
    Retorna: (col_periodico_id, col_url, source_system_default)
    """
    cols = set(fieldnames)
    if {"periodico_id", "url_principal_atual"} <= cols:
        return "periodico_id", "url_principal_atual", "periodicos"
    if {"periodico_id", "base_url"} <= cols:
        return "periodico_id", "base_url", "titledb"
    raise ValueError(
        "Arquivo não reconhecido. Esperado CSV com colunas "
        "`periodico_id` + `url_principal_atual` ou `periodico_id` + `base_url`."
    )


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def validate_url(
    periodico_id: str,
    url: str,
    source_system: str,
    session: requests.Session,
    timeout: int,
    user_agent: str,
    verify_ssl: bool = True,
) -> Dict[str, Optional[str]]:
    headers = {"User-Agent": user_agent}
    original_domain = extract_domain(url)

    status_code = None
    redirect_count = 0
    final_url = None
    final_domain = None
    domain_changed = None
    https_valid = None
    timeout_flag = False
    ojs_detected = None
    ojs_version_hint = None
    is_accessible = None
    problem_type = None
    notes = None
    exception_name = None

    try:
        response = session.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
            verify=verify_ssl,
        )
        status_code = response.status_code
        redirect_count = len(response.history)
        final_url = response.url
        final_domain = extract_domain(final_url)
        domain_changed = (
            original_domain != final_domain
            if original_domain is not None and final_domain is not None
            else None
        )
        is_accessible = 200 <= response.status_code < 400
        https_valid = True if str(final_url).startswith("https://") else None

        html_text = ""
        ctype = response.headers.get("Content-Type", "")
        if "html" in ctype.lower():
            response.encoding = response.apparent_encoding or response.encoding
            html_text = response.text

        ojs_detected, ojs_version_hint = guess_ojs(html_text, dict(response.headers))
        problem_type = classify_problem(
            status_code=status_code,
            timeout=False,
            exception_name=None,
            is_accessible=is_accessible,
            domain_changed=domain_changed,
            redirect_count=redirect_count,
        )

    except requests.exceptions.SSLError as exc:
        exception_name = exc.__class__.__name__
        https_valid = False
        is_accessible = False
        problem_type = "ssl_error"
        notes = str(exc)[:500]
    except requests.exceptions.Timeout as exc:
        exception_name = exc.__class__.__name__
        timeout_flag = True
        is_accessible = False
        problem_type = "timeout"
        notes = str(exc)[:500]
    except requests.exceptions.RequestException as exc:
        exception_name = exc.__class__.__name__
        is_accessible = False
        problem_type = classify_problem(
            status_code=None,
            timeout=False,
            exception_name=exception_name,
            is_accessible=False,
            domain_changed=None,
            redirect_count=0,
        )
        notes = str(exc)[:500]

    url_check_id = hash_id(
        "URL_",
        [periodico_id, source_system, url, final_url, status_code, problem_type],
    )

    return {
        "url_check_id": url_check_id,
        "periodico_id": periodico_id,
        "source_system": source_system,
        "url_original": url,
        "url_original_domain": original_domain,
        "check_date": None,
        "http_status": status_code,
        "redirect_count": redirect_count,
        "final_url": final_url,
        "final_domain": final_domain,
        "domain_changed": domain_changed,
        "https_valid": https_valid,
        "timeout": timeout_flag,
        "ojs_detected": ojs_detected,
        "ojs_version_hint": ojs_version_hint,
        "is_accessible": is_accessible,
        "problem_type": problem_type,
        "notes": notes,
        "created_at": None,
    }


def write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_summary(results: List[Dict[str, object]]) -> List[Dict[str, object]]:
    total = len(results)
    by_problem: Dict[str, int] = {}
    accessible = 0
    changed_domain = 0
    redirects = 0
    ojs_detected = 0

    for row in results:
        problem = row.get("problem_type") or "unknown"
        by_problem[problem] = by_problem.get(problem, 0) + 1
        if normalize_bool(row.get("is_accessible")):
            accessible += 1
        if normalize_bool(row.get("domain_changed")):
            changed_domain += 1
        try:
            if int(row.get("redirect_count") or 0) > 0:
                redirects += 1
        except Exception:
            pass
        if normalize_bool(row.get("ojs_detected")):
            ojs_detected += 1

    summary = [
        {"metric": "total_urls", "value": total},
        {"metric": "accessible_urls", "value": accessible},
        {"metric": "urls_with_redirect", "value": redirects},
        {"metric": "urls_with_domain_changed", "value": changed_domain},
        {"metric": "urls_with_ojs_detected", "value": ojs_detected},
    ]
    for k, v in sorted(by_problem.items()):
        summary.append({"metric": f"problem_type::{k}", "value": v})

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Validador de URLs para Cariniana")
    parser.add_argument("input_csv", type=Path, help="CSV de entrada")
    parser.add_argument("--outdir", type=Path, default=Path("."), help="Diretório de saída")
    parser.add_argument("--source-system", type=str, default=None, help="Sobrescreve source_system")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Timeout por requisição")
    parser.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT, help="User-Agent HTTP")
    parser.add_argument("--insecure", action="store_true", help="Desabilita validação SSL")
    args = parser.parse_args()

    rows = load_rows(args.input_csv)
    if not rows:
        raise SystemExit("CSV vazio.")

    periodico_col, url_col, inferred_source = choose_input_columns(list(rows[0].keys()))
    source_system = args.source_system or inferred_source

    # Deduplica por periodico_id + url
    seen = set()
    work_rows = []
    for row in rows:
        periodico_id = (row.get(periodico_col) or "").strip()
        url = (row.get(url_col) or "").strip()
        if not periodico_id or not url:
            continue
        key = (periodico_id, url)
        if key in seen:
            continue
        seen.add(key)
        work_rows.append({"periodico_id": periodico_id, "url": url})

    session = requests.Session()
    results: List[Dict[str, object]] = []

    for item in work_rows:
        result = validate_url(
            periodico_id=item["periodico_id"],
            url=item["url"],
            source_system=source_system,
            session=session,
            timeout=args.timeout,
            user_agent=args.user_agent,
            verify_ssl=not args.insecure,
        )
        results.append(result)

    args.outdir.mkdir(parents=True, exist_ok=True)

    infra_fields = [
        "url_check_id",
        "periodico_id",
        "source_system",
        "url_original",
        "url_original_domain",
        "check_date",
        "http_status",
        "redirect_count",
        "final_url",
        "final_domain",
        "domain_changed",
        "https_valid",
        "timeout",
        "ojs_detected",
        "ojs_version_hint",
        "is_accessible",
        "problem_type",
        "notes",
        "created_at",
    ]
    summary_fields = ["metric", "value"]

    write_csv(args.outdir / "infraestrutura_url.csv", results, infra_fields)
    write_csv(args.outdir / "resumo_validacao_urls.csv", build_summary(results), summary_fields)

    print(f"URLs processadas: {len(work_rows)}")
    print(f"Resultados gravados em: {args.outdir.resolve()}")


if __name__ == "__main__":
    main()
