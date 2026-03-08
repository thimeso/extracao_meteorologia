"""
Script responsável pela extração de dados pluviométricos disponibilizados
pelo CEMADEN (Centro Nacional de Monitoramento e Alertas de Desastres Naturais).

O script automatiza o download de arquivos disponibilizados pelo portal do
CEMADEN, realiza a leitura e transformação inicial dos dados e salva os
resultados em formatos estruturados (CSV e Parquet) para utilização em
pipelines de dados ou projetos de análise climática.

Este código foi desenvolvido para servir como etapa de ingestão de dados
meteorológicos em um pipeline de engenharia de dados.

Fonte de dados:
https://www.cemaden.gov.br/

Autor: Projeto de ingestão meteorológica
"""

# ==============================
# 1) BIBLIOTECAS
# ==============================

from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urljoin, urldefrag

import requests
from bs4 import BeautifulSoup


# ==============================
# 2) CONFIGURAÇÕES
# ==============================

RADARES_TODOS = [
    "almenara",
    "jaraguari",
    "maceio",
    "natal",
    "petrolina",
    "salvador",
    "santa_teresa",
    "sao_francisco",
    "tres_marias",
]

# Escolha quais baixar (edite essa lista)
RADARES_SELECIONADOS = [
    # "almenara",
    "jaraguari"
    # "maceio",
    # "natal",
    # "petrolina",
    # "salvador",
    # "santa_teresa",
    # "sao_francisco",
    # "tres_marias",
]

BASE = "https://mapainterativo.cemaden.gov.br"
START_FMT = BASE + "/download/downradares.php?radar={radar}"

DOWNLOAD_ROOT = Path("data")
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

# Seu ambiente está sem CA confiável, usar False.
VERIFY_SSL = False

# Se quiser restringir extensões, configure aqui. Se quiser “tudo”, coloque None.
ALLOWED_EXTS = {".azi", ".vol", ".zip", ".gz", ".csv", ".txt"}

# Limites anti-loop
MAX_PAGES_PER_RADAR = 40        # quantas páginas HTML no máximo visitar por radar
MAX_DOWNLOADS_PER_RADAR = 500   # trava segurança (evita baixar infinito)
SLEEP_BETWEEN_REQUESTS = 0.2    # respeitar o servidor


# ==============================
# 3) FUNÇÕES
# ==============================

def normalize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)  # inválidos no Windows
    name = re.sub(r"\s+", "_", name)
    return name[:180]


def extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        abs_url, _ = urldefrag(abs_url)
        # mantém só dentro do mesmo host
        if urlparse(abs_url).netloc and urlparse(abs_url).netloc != urlparse(BASE).netloc:
            continue
        links.append(abs_url)
    return links


def is_download_candidate(url: str) -> bool:
    """
    Decide se o link "parece" arquivo:
    - se tiver produto=... com extensão
    - ou path termina com extensão
    """
    p = urlparse(url)
    qs = parse_qs(p.query)

    prod = (qs.get("produto") or [None])[0]
    if prod:
        ext = Path(prod).suffix.lower()
        if ALLOWED_EXTS is None:
            return ext != ""
        return ext in ALLOWED_EXTS

    ext = Path(p.path).suffix.lower()
    if ext:
        if ALLOWED_EXTS is None:
            return True
        return ext in ALLOWED_EXTS

    return False


def file_name_for_url(url: str, radar: str) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)
    prod = (qs.get("produto") or [None])[0]

    if prod:
        base = f"{radar}__{prod}"
    else:
        base = f"{radar}__{Path(p.path).name or 'download'}"

    return normalize_filename(base)


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    v = float(n)
    for u in units:
        if v < 1024 or u == units[-1]:
            return f"{v:.1f} {u}" if u != "B" else f"{int(v)} B"
        v /= 1024
    return f"{n} B"


def download_with_status(session: requests.Session, url: str, radar: str) -> Path | None:
    """
    - printa o URL
    - tenta baixar
    - mostra status/tamanho
    - salva em pasta única com prefixo do radar
    - retorna Path se baixou, None se não era arquivo
    """
    print(f"\n Vai baixar: {url}")

    r = session.get(url, stream=True, timeout=180, verify=VERIFY_SSL)
    print(f"   HTTP: {r.status_code}")

    # Se não for 200, aborta
    if r.status_code != 200:
        return None

    ctype = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ctype:
        print("   Retornou HTML (provável menu/página), não é arquivo.")
        return None

    out_name = file_name_for_url(url, radar)
    out_path = DOWNLOAD_ROOT / out_name

    # evita baixar de novo
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"    Já existe, pulando: {out_path} ({human_size(out_path.stat().st_size)})")
        return out_path

    total = 0
    content_len = r.headers.get("Content-Length")
    expected = int(content_len) if content_len and content_len.isdigit() else None

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 512):
            if chunk:
                f.write(chunk)
                total += len(chunk)

    print(f"   Download OK: {out_path} ({human_size(total)})" + (f" / esperado {human_size(expected)}" if expected else ""))

    # proteção: arquivo muito pequeno pode ser erro/placeholder
    if total < 200:  # ajuste se quiser
        print("   Arquivo muito pequeno (<200B). Pode ser placeholder/erro.")
    return out_path


# =========================
# 4) CRAWL CONTROLADO POR RADAR (SEM LOOP INFINITO)
# =========================
def crawl_radar_controlado(radar: str) -> list[Path]:
    """
    Estratégia:
    - começa na página do radar
    - visita no máximo MAX_PAGES_PER_RADAR páginas HTML dentro de /download/
    - em cada página, tenta baixar apenas links que parecem arquivo
    - qualquer link que NÃO pareça arquivo vira candidato a página (para pegar "produtos" e outros índices)
    - dedupe total de páginas e downloads
    """
    start_url = START_FMT.format(radar=radar)
    print(f"\n==============================")
    print(f" Radar: {radar}")
    print(f" Start: {start_url}")
    print(f" Saída: {DOWNLOAD_ROOT}")
    print(f"==============================")

    session = requests.Session()
    session.headers.update({"User-Agent": "thiago-cemaden-downloader/1.0"})

    to_visit = [start_url]
    visited_pages: set[str] = set()
    attempted_downloads: set[str] = set()
    downloaded_paths: list[Path] = []

    while to_visit and len(visited_pages) < MAX_PAGES_PER_RADAR and len(downloaded_paths) < MAX_DOWNLOADS_PER_RADAR:
        page_url = to_visit.pop(0)

        # filtro: fica só em /download/
        if not urlparse(page_url).path.startswith("/download/"):
            continue

        if page_url in visited_pages:
            continue

        visited_pages.add(page_url)
        print(f"\n Página ({len(visited_pages)}/{MAX_PAGES_PER_RADAR}): {page_url}")

        resp = session.get(page_url, timeout=60, verify=VERIFY_SSL)
        print(f"   HTTP: {resp.status_code} | Content-Type: {resp.headers.get('Content-Type')}")
        if resp.status_code != 200:
            continue

        ctype = (resp.headers.get("Content-Type") or "").lower()

        # Se por algum motivo a "página" já for um arquivo
        if "text/html" not in ctype:
            if page_url not in attempted_downloads:
                attempted_downloads.add(page_url)
                p = download_with_status(session, page_url, radar)
                if p:
                    downloaded_paths.append(p)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        links = extract_links(resp.text, page_url)

        # Deduplicar links mantendo ordem
        seen = set()
        links = [x for x in links if not (x in seen or seen.add(x))]

        for link in links:
            # filtro: só /download/
            if not urlparse(link).path.startswith("/download/"):
                continue

            # tenta baixar arquivos
            if is_download_candidate(link):
                if link in attempted_downloads:
                    continue
                attempted_downloads.add(link)

                p = download_with_status(session, link, radar)
                if p:
                    downloaded_paths.append(p)

                if len(downloaded_paths) >= MAX_DOWNLOADS_PER_RADAR:
                    print(" Atingiu MAX_DOWNLOADS_PER_RADAR, parando por segurança.")
                    break

                time.sleep(SLEEP_BETWEEN_REQUESTS)

            else:
                # candidata a página (ex: "produtos", índices, etc.)
                if link not in visited_pages and link not in to_visit:
                    to_visit.append(link)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"\n Final radar={radar}: {len(downloaded_paths)} arquivos baixados | {len(visited_pages)} páginas visitadas")
    return downloaded_paths


def main():
    # valida seleção
    invalid = [r for r in RADARES_SELECIONADOS if r not in RADARES_TODOS]
    if invalid:
        raise ValueError(f"Radares inválidos em RADARES_SELECIONADOS: {invalid}")

    total_files = 0
    for radar in RADARES_SELECIONADOS:
        files = crawl_radar_controlado(radar)
        total_files += len(files)

    print(f"\n Concluído. Total de arquivos baixados: {total_files}")
    print(f" Pasta única: {DOWNLOAD_ROOT}")

# ==============================
# 5) EXECUÇÃO
# ==============================

if __name__ == "__main__":
    main()