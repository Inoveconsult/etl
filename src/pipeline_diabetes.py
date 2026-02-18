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


def carregar_tratar_dados_diabeticos(
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
        "TER PELO MENOS 01 (UMA) CONSULTA PRESENCIAL OU REMOTA REALIZADAS POR MÉDICA(O) OU ENFERMEIRA(O),NOS ÚLTIMOS 06 (SEIS) MESES",
        "TER PELO MENOS 01 (UM) REGISTRO DE AFERIÇÃO DE PRESSÃO ARTERIAL REALIZADO NOS ÚLTIMOS 06 (SEIS) MESES",
        "TER PELO MENOS 01 (UM) REGISTRO SIMULTÂNEOS DE PESO E ALTURA REALIZADO NOS ÚLTIMOS 12 (DOZE) MESES",
        "TER PELO MENOS 02 (DUAS) VISITAS DOMICILIARES REALIZADAS POR ACS/TACS,COM INTERVALO MÍNIMO DE 30 (TRINTA) DIAS,NOS ÚLTIMOS 12 (DOZE) MESES",
        "TER PELO MENOS 01 (UM) REGISTRO DE SOLICITAÇÃO DE HEMOGLOBINA GLICADA REALIZADA OU AVALIADA,NOS ÚLTIMOS 12 (DOZE) MESES",
        "TER PELO MENOS 01 (UMA) AVALIAÇÃO DOS PÉS REALIZADA NOS ÚLTIMOS 12 (DOZE) MESES",
        "SOMATÓRIO DAS BOAS PRÁTICAS PONTUADAS PARA A PESSOA COM DIABETES NO PERÍODO",
        "Nº TOTAL DE PESSOAS COM DIABETES VINCULADAS À EQUIPE NO PERÍODO"        
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
