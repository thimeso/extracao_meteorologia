"""
inmet_dados_historicos.py

Script responsável pela ingestão de dados históricos do INMET
(Instituto Nacional de Meteorologia), com foco nos arquivos anuais
compactados disponibilizados pelo portal oficial.

O pipeline segue uma estrutura de três camadas:

1. RAW
   - Armazena o arquivo ZIP original baixado da fonte.
2. INT
   - Consolida e normaliza os arquivos CSV internos.
3. EXP
   - Exporta o conjunto final em um único arquivo analítico.

O objetivo deste script é disponibilizar uma base tratada e estruturada
para consumo em projetos de análise climática, engenharia de dados e
visualização de indicadores meteorológicos.

Fonte de dados:
https://portal.inmet.gov.br/dadoshistoricos

Autor: Projeto de ingestão meteorológica
"""

# ==============================
# 1) BIBLIOTECAS
# ==============================

from pathlib import Path
from datetime import datetime
import zipfile
import pandas as pd
import requests
import certifi


# ==============================
# 2) CONFIGURAÇÕES
# ==============================

ANOS = [2024]
BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos"

BASE_PATH = Path("data")
RAW_PATH = BASE_PATH / "raw" / "inmet"
INT_PATH = BASE_PATH / "int" / "inmet"
EXP_PATH = BASE_PATH / "exp" / "inmet"
LOG_PATH = BASE_PATH / "logs" / "ingestion_log.csv"

for path in [RAW_PATH, INT_PATH, EXP_PATH, LOG_PATH.parent]:
    path.mkdir(parents=True, exist_ok=True)

VERIFY_SSL = certifi.where()


# ==============================
# 3) LOG DE INGESTÃO
# ==============================

def log_ingestion(fonte: str, arquivo: str, ano: int, status: str, linhas: int | None = None, erro: str | None = None) -> None:
    """
    Registra eventos de execução do pipeline em arquivo CSV.

    Parameters
    ----------
    fonte : str
        Nome da fonte de dados.
    arquivo : str
        Nome do arquivo processado.
    ano : int
        Ano de referência do processamento.
    status : str
        Status da etapa executada.
    linhas : int | None
        Quantidade de linhas geradas na etapa.
    erro : str | None
        Mensagem de erro, se houver.
    """

    now = datetime.now().isoformat(timespec="seconds")

    new_row = pd.DataFrame([{
        "fonte": fonte,
        "arquivo": arquivo,
        "ano": ano,
        "data_execucao": now,
        "status": status,
        "linhas": linhas,
        "erro": erro
    }])

    if LOG_PATH.exists():
        df_log = pd.read_csv(LOG_PATH)
        df_log = pd.concat([df_log, new_row], ignore_index=True)
    else:
        df_log = new_row

    df_log.to_csv(LOG_PATH, index=False)


# ==============================
# 4) CAMADA RAW
# ==============================

def download_ano(ano: int) -> Path | None:
    """
    Realiza o download do arquivo histórico anual do INMET.

    Parameters
    ----------
    ano : int
        Ano de referência do arquivo.

    Returns
    -------
    Path | None
        Caminho do arquivo ZIP baixado, ou None em caso de erro.
    """

    url = f"{BASE_URL}/{ano}.zip"
    out_dir = RAW_PATH / f"ano={ano}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"dados_{ano}.zip"

    if out_path.exists():
        print(f"RAW já existe: {out_path}")
        return out_path

    print(f"Baixando {url}")

    try:
        response = requests.get(url, timeout=120, verify=VERIFY_SSL)
        response.raise_for_status()

        with open(out_path, "wb") as file:
            file.write(response.content)

        log_ingestion("inmet", out_path.name, ano, "download_ok")
        return out_path

    except Exception as exc:
        log_ingestion("inmet", out_path.name, ano, "erro_download", erro=str(exc))
        return None


# ==============================
# 5) CAMADA INT
# ==============================

def processar_zip(zip_path: Path, ano: int) -> pd.DataFrame:
    """
    Processa o arquivo ZIP anual do INMET, lê os CSVs internos,
    extrai metadados das oito primeiras linhas e consolida todos
    os arquivos em um único DataFrame.

    Parameters
    ----------
    zip_path : Path
        Caminho do arquivo ZIP.
    ano : int
        Ano de referência.

    Returns
    -------
    DataFrame
        DataFrame consolidado da camada INT.
    """

    print(f"Processando ZIP INT: {zip_path.name}")

    dfs = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        csv_files = [f for f in zip_ref.namelist() if f.lower().endswith(".csv")]
        total_csv = len(csv_files)

        print(f"Total de tabelas (CSV) no ZIP: {total_csv}")

        for idx, csv_file in enumerate(csv_files, start=1):
            print(f"Processando {idx} de {total_csv}: {csv_file}")

            with zip_ref.open(csv_file) as file:
                header_lines = [
                    file.readline().decode("latin-1").strip()
                    for _ in range(8)
                ]

            metadata = {}
            for line in header_lines:
                if ":" in line:
                    key, value = line.split(":", 1)
                    metadata[key.strip()] = value.strip()

            df_data = pd.read_csv(
                zip_ref.open(csv_file),
                sep=";",
                skiprows=8,
                encoding="latin-1",
                decimal=","
            )

            df_meta = pd.DataFrame([metadata] * len(df_data))
            df_final_file = pd.concat([df_meta, df_data], axis=1)

            dfs.append(df_final_file)

    df_final = pd.concat(dfs, ignore_index=True)

    out_file = INT_PATH / f"inmet_{ano}.parquet"
    df_final.to_parquet(out_file, index=False)

    print(f"INT finalizado ({len(df_final)} linhas consolidadas)")

    log_ingestion("inmet", zip_path.name, ano, "processado", linhas=len(df_final))

    return df_final


# ==============================
# 6) CAMADA EXP
# ==============================

def exportar_exp_unico(df: pd.DataFrame, ano: int) -> None:
    """
    Exporta a camada EXP em um único arquivo Parquet.

    Parameters
    ----------
    df : DataFrame
        DataFrame consolidado da camada INT.
    ano : int
        Ano de referência.
    """

    print("Iniciando exportação EXP em arquivo único")

    df["DATAHORA"] = pd.to_datetime(
        df["Data"].astype(str) + " " + df["Hora UTC"].astype(str),
        errors="coerce"
    )

    df["ano"] = df["DATAHORA"].dt.year
    df["mes"] = df["DATAHORA"].dt.month

    df_exp = df[df["ano"] == ano].copy()

    total = len(df_exp)
    print(f"Total de linhas para exportar (ano={ano}): {total}")

    out_file = EXP_PATH / f"inmet_exp_{ano}.parquet"
    df_exp.to_parquet(out_file, index=False)

    print(f"EXP finalizado: {out_file}")


# ==============================
# 7) PIPELINE PRINCIPAL
# ==============================

def pipeline_inmet(ano: int) -> None:
    """
    Executa o pipeline completo de ingestão para um ano específico.

    Parameters
    ----------
    ano : int
        Ano de referência.
    """

    zip_path = download_ano(ano)
    if not zip_path:
        return

    df = processar_zip(zip_path, ano)
    exportar_exp_unico(df, ano)

    print(f"Pipeline finalizado para {ano}")


# ==============================
# 8) EXECUÇÃO
# ==============================

if __name__ == "__main__":
    for ano in ANOS:
        pipeline_inmet(ano)