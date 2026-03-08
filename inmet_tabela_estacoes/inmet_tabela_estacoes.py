"""
inmet_tabela_estacoes.py

Script responsável pela extração de dados tabulares de estações meteorológicas
a partir da interface web do INMET (Tabela de Dados das Estações), utilizando
automação com Selenium.

O fluxo automatiza:

1. abertura da página da estação
2. abertura do menu lateral de filtros
3. preenchimento do intervalo de datas
4. geração da tabela
5. extração manual dos dados renderizados em HTML
6. normalização das colunas
7. exportação em CSV e Parquet

Importante:
O código A001 utilizado na URL é apenas um exemplo de estação.
O script pode ser adaptado para qualquer outra estação suportada pelo portal,
desde que a URL correspondente seja utilizada.

Fonte:
https://tempo.inmet.gov.br/TabelaEstacoes/A001

Autor: Projeto de ingestão meteorológica
"""

# ==============================
# 1) BIBLIOTECAS
# ==============================

from pathlib import Path
from datetime import datetime
import time
import re

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


# ==============================
# 2) CONFIGURAÇÕES
# ==============================

URL = "https://tempo.inmet.gov.br/TabelaEstacoes/A001"

DATA_INICIO = "2026-03-08"
DATA_FIM = "2026-03-08"

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 30


# ==============================
# 3) FUNÇÕES AUXILIARES
# ==============================

def sanitize_filename(text: str) -> str:
    """
    Normaliza nomes de arquivos removendo caracteres inválidos.

    Parameters
    ----------
    text : str
        Texto base para o nome do arquivo.

    Returns
    -------
    str
        Nome de arquivo normalizado.
    """

    text = re.sub(r'[\\/:*?"<>|]+', "_", text)
    text = re.sub(r"\s+", "_", text).strip("_")
    return text


def make_driver() -> webdriver.Chrome:
    """
    Cria e retorna uma instância do navegador Chrome configurada
    para execução automatizada.

    Returns
    -------
    webdriver.Chrome
        Instância do ChromeDriver.
    """

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=pt-BR")
    options.add_argument("--window-size=1600,1000")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def js_click(driver: webdriver.Chrome, element) -> None:
    """
    Executa clique via JavaScript em um elemento.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    element : WebElement
        Elemento a ser clicado.
    """

    driver.execute_script("arguments[0].click();", element)


def wait_page_ready(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Aguarda a página inicial terminar de carregar.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    wait : WebDriverWait
        Objeto de espera explícita.
    """

    print("Aguardando a tela terminar de carregar...")
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(), 'Data de Referência') or contains(text(), 'Estação:')]")
        )
    )
    time.sleep(3)


def abrir_menu_filtros(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Abre o menu lateral de filtros da interface.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    wait : WebDriverWait
        Objeto de espera explícita.
    """

    print("Abrindo menu lateral de filtros...")

    menu_icon = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//i[contains(@class,'bars') and contains(@class,'icon') and contains(@class,'header-icon')]")
        )
    )

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", menu_icon)
    time.sleep(0.3)
    js_click(driver, menu_icon)
    time.sleep(1.5)

    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(), 'Produto')]")
        )
    )

    print("Menu lateral aberto.")


def localizar_inputs_data(driver: webdriver.Chrome, wait: WebDriverWait):
    """
    Localiza os dois campos de data visíveis no menu lateral.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    wait : WebDriverWait
        Objeto de espera explícita.

    Returns
    -------
    tuple
        Elementos correspondentes às datas de início e fim.
    """

    inputs = wait.until(
        lambda d: [el for el in d.find_elements(By.XPATH, "//input[@type='date']") if el.is_displayed()]
    )

    if len(inputs) < 2:
        raise RuntimeError(f"Encontrei apenas {len(inputs)} input(s) type='date' visíveis.")

    return inputs[0], inputs[1]


def preencher_data(driver: webdriver.Chrome, element, valor: str) -> None:
    """
    Preenche um campo de data HTML.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    element : WebElement
        Campo de data.
    valor : str
        Valor da data no formato YYYY-MM-DD.
    """

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
    time.sleep(0.2)

    driver.execute_script("""
        arguments[0].value = arguments[1];
        arguments[0].dispatchEvent(new Event('input', {bubbles: true}));
        arguments[0].dispatchEvent(new Event('change', {bubbles: true}));
        arguments[0].dispatchEvent(new Event('blur', {bubbles: true}));
    """, element, valor)

    time.sleep(0.3)


def validar_datas_preenchidas(driver: webdriver.Chrome, inicio_esperado: str, fim_esperado: str) -> None:
    """
    Valida se os dois campos de data foram preenchidos corretamente.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    inicio_esperado : str
        Valor esperado da data inicial.
    fim_esperado : str
        Valor esperado da data final.
    """

    vals = [
        el.get_attribute("value")
        for el in driver.find_elements(By.XPATH, "//input[@type='date']")
        if el.is_displayed()
    ]

    if len(vals) < 2:
        raise RuntimeError("Não consegui validar os dois campos de data.")

    if vals[0] != inicio_esperado or vals[1] != fim_esperado:
        raise RuntimeError(f"Datas não ficaram preenchidas corretamente. Valores atuais: {vals}")


def esperar_overlay_sumir(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Aguarda o desaparecimento de overlays de carregamento.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.
    wait : WebDriverWait
        Objeto de espera explícita.
    """

    try:
        wait.until(
            lambda d: len(d.find_elements(By.XPATH, "//*[contains(@class,'dimmer') and contains(@class,'active')]")) == 0
        )
    except Exception:
        pass

    try:
        wait.until(
            lambda d: len(d.find_elements(By.XPATH, "//*[contains(@class,'loading')]")) == 0
        )
    except Exception:
        pass


def extrair_nome_estacao(driver: webdriver.Chrome) -> str:
    """
    Extrai o nome da estação exibido na página.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.

    Returns
    -------
    str
        Nome da estação.
    """

    try:
        el = driver.find_element(By.XPATH, "//*[contains(text(), 'Estação:')]")
        txt = el.text.replace("Estação:", "").strip()
        return txt if txt else "A001"
    except Exception:
        return "A001"


def extrair_tabela_manual(driver: webdriver.Chrome) -> pd.DataFrame:
    """
    Extrai manualmente os dados da tabela HTML renderizada na página.

    Parameters
    ----------
    driver : webdriver.Chrome
        Instância do navegador.

    Returns
    -------
    DataFrame
        Dados da tabela em formato tabular bruto.
    """

    print("Extraindo tabela manualmente...")

    tabela = driver.find_element(By.TAG_NAME, "table")
    linhas = tabela.find_elements(By.TAG_NAME, "tr")

    dados = []

    for linha in linhas:
        colunas = linha.find_elements(By.TAG_NAME, "td")
        if colunas:
            valores = [c.text.strip() for c in colunas]
            if any(v != "" for v in valores):
                dados.append(valores)

    if not dados:
        raise RuntimeError("Nenhuma linha de dados foi extraída da tabela.")

    return pd.DataFrame(dados)


def aplicar_schema_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica o schema final padronizado conforme a estrutura da tabela do INMET.

    Parameters
    ----------
    df : DataFrame
        DataFrame bruto extraído da tabela.

    Returns
    -------
    DataFrame
        DataFrame com nomes de colunas normalizados.
    """

    colunas_final = [
        "data",
        "hora_utc",
        "temperatura_c_inst",
        "temperatura_c_max",
        "temperatura_c_min",
        "umidade_perc_inst",
        "umidade_perc_max",
        "umidade_perc_min",
        "pto_orvalho_c_inst",
        "pto_orvalho_c_max",
        "pto_orvalho_c_min",
        "pressao_hpa_inst",
        "pressao_hpa_max",
        "pressao_hpa_min",
        "vento_vel_ms",
        "vento_dir_graus",
        "vento_raj_ms",
        "radiacao_kj_m2",
        "chuva_mm",
    ]

    qtd_cols = df.shape[1]

    if qtd_cols < len(colunas_final):
        faltantes = len(colunas_final) - qtd_cols
        for _ in range(faltantes):
            df[df.shape[1]] = None
    elif qtd_cols > len(colunas_final):
        df = df.iloc[:, :len(colunas_final)].copy()

    df.columns = colunas_final

    return df


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas numéricas e cria coluna de data/hora consolidada.

    Parameters
    ----------
    df : DataFrame
        DataFrame com schema final.

    Returns
    -------
    DataFrame
        DataFrame com tipos normalizados.
    """

    cols_texto = [
        "data",
        "hora_utc",
        "estacao",
        "url_origem",
        "data_extracao",
        "data_inicio_consulta",
        "data_fim_consulta",
    ]

    cols_numericas = [c for c in df.columns if c not in cols_texto]

    for col in cols_numericas:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .replace({"": None, "None": None, "nan": None})
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "data" in df.columns and "hora_utc" in df.columns:
        df["datahora_utc"] = pd.to_datetime(
            df["data"].astype(str) + " " + df["hora_utc"].astype(str).str.zfill(4),
            format="%d/%m/%Y %H%M",
            errors="coerce"
        )

    return df


# ==============================
# 4) FUNÇÃO PRINCIPAL
# ==============================

def main() -> None:
    """
    Executa o pipeline completo de extração tabular da página do INMET.
    """

    driver = make_driver()
    wait = WebDriverWait(driver, TIMEOUT)

    try:
        print(f"Abrindo {URL}")
        driver.get(URL)

        wait_page_ready(driver, wait)
        abrir_menu_filtros(driver, wait)

        print(f"Preenchendo datas: {DATA_INICIO} até {DATA_FIM}")
        data_inicio_input, data_fim_input = localizar_inputs_data(driver, wait)

        preencher_data(driver, data_inicio_input, DATA_INICIO)
        preencher_data(driver, data_fim_input, DATA_FIM)

        validar_datas_preenchidas(driver, DATA_INICIO, DATA_FIM)
        print("Datas preenchidas com sucesso.")

        print("Clicando em 'Gerar Tabela'...")
        btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Gerar Tabela')]"))
        )
        js_click(driver, btn)

        print("Aguardando atualização da tabela...")
        esperar_overlay_sumir(driver, wait)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        time.sleep(4)

        df = extrair_tabela_manual(driver)
        df = aplicar_schema_final(df)

        nome_estacao = extrair_nome_estacao(driver)
        df["data_inicio_consulta"] = DATA_INICIO
        df["data_fim_consulta"] = DATA_FIM
        df["data_extracao"] = datetime.now().isoformat(timespec="seconds")
        df["estacao"] = nome_estacao
        df["url_origem"] = URL

        df = converter_tipos(df)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome = sanitize_filename(f"{nome_estacao}_{DATA_INICIO}_{DATA_FIM}_{ts}")

        csv_path = OUT_DIR / f"{nome}.csv"
        parquet_path = OUT_DIR / f"{nome}.parquet"

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        df.to_parquet(parquet_path, index=False)

        print(f"CSV salvo em: {csv_path}")
        print(f"Parquet salvo em: {parquet_path}")
        print(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")

        print("Lendo arquivo Parquet salvo...")
        df_parquet = pd.read_parquet(parquet_path)

        print("\nPreview do Parquet:")
        print(df_parquet.head(10).to_string(index=False))

        print("\nSchema final:")
        print(df_parquet.dtypes.to_string())

    finally:
        print("Fechando navegador...")
        driver.quit()


# ==============================
# 5) EXECUÇÃO
# ==============================

if __name__ == "__main__":
    main()