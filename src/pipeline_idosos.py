import pandas as pd
import unicodedata
from src.utils import (
    converte_numero,
    normalizar_codigo,
    remover_ultimas_linhas
)

def _normalizar_nome_coluna(col):
    """
    Remove acento, espaços extras e padroniza para UPPER.
    """
    col = str(col).strip().upper()
    col = unicodedata.normalize("NFKD", col)
    col = "".join(c for c in col if not unicodedata.combining(c))
    return col


def carregar_tratar_dados_idosos(
    caminho_arquivo,
    faixas,
    coluna_origem_excel,
    coluna_pontuacao,
    skiprows=17
):
    """
    Pipeline robusto para tratamento dos dados MAIS ACESSO.
    """

    # =========================
    # 1️⃣ Leitura
    # =========================
    df = pd.read_excel(caminho_arquivo, skiprows=skiprows)

    # =========================
    # 2️⃣ Normalização de colunas
    # =========================
    df.columns = [_normalizar_nome_coluna(c) for c in df.columns]

    coluna_origem_excel = _normalizar_nome_coluna(coluna_origem_excel)

    # =========================
    # 3️⃣ Validação de coluna obrigatória
    # =========================
    if coluna_origem_excel not in df.columns:
        raise ValueError(
            f"Coluna '{coluna_origem_excel}' não encontrada.\n"
            f"Colunas disponíveis: {list(df.columns)}"
        )

    # =========================
    # 4️⃣ Remoção segura de colunas desnecessárias
    # =========================
    colunas_remover = [
        "CNES",
        "ESTABELECIMENTO",
        "TIPO DO ESTABELECIMENTO",
        "SIGLA DA EQUIPE",
        "TER REGISTRO DE PELO MENOS 01 CONSULTA PRESENCIAL OU REMOTA POR PROFISSIONAL MÉDICA(O) OU ENFERMEIRA(O) REALIZADA NOS ÚLTIMOS 12 MESES",
        "TER REALIZADO PELO MENOS 01 (UM) REGISTRO SIMULTÂNEO (NO MESMO DIA) DE PESO E ALTURA PARA AVALIAÇÃO ANTROPOMÉTRICA NOS ÚLTIMOS 12 MESES",
        "TER REGISTRO DE PELO MENOS 02 VISITAS DOMICILIARES POR ACS/TACS,COM INTERVALO MÍNIMO DE 30 DIAS,REALIZADAS NOS ÚLTIMOS 12 MESES AÇÃO",
        "TER REGISTRO DE 1 DOSE DA VACINA CONTRA INFLUENZA REALIZADA NOS ÚLTIMOS 12 MESES",
        "SOMATÓRIO DE BOAS PRÁTICAS REALIZADAS PARA PESSOAS IDOSAS VINCULADAS À EQUIPE",
        "NÚMERO TOTAL DE PESSOAS IDOSAS VINCULADAS À EQUIPE"       
    ]

    colunas_remover = [_normalizar_nome_coluna(c) for c in colunas_remover]

    df = df.drop(columns=colunas_remover, errors="ignore")

    # =========================
    # 5️⃣ Remover linhas finais
    # =========================
    df = remover_ultimas_linhas(df, n=2)

    # =========================
    # 6️⃣ Renomear coluna de pontuação
    # =========================
    df = df.rename(columns={coluna_origem_excel: coluna_pontuacao})

    # =========================
    # 7️⃣ Converter para número
    # =========================
    df = converte_numero(df, coluna_pontuacao)

    # =========================
    # 8️⃣ Normalizar INE
    # =========================
    if "INE" not in df.columns:
        raise ValueError("Coluna 'INE' não encontrada no arquivo.")

    df = normalizar_codigo(df, "INE")

    return df

