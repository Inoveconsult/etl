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


def carregar_tratar_dados_criancas(
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
        "TER A 1ª CONSULTA PRESENCIAL REALIZADA POR MÉDICA(O) OU ENFERMEIRA(O), ATÉ O 30º DIA DE VIDA",
        "TER PELO MENOS 09 (NOVE) CONSULTAS PRESENCIAIS OU REMOTAS REALIZADAS POR MÉDICA (O) OU ENFERMEIRA(O) ATÉ DOIS ANOS DE VIDA",
        "TER PELO MENOS 09 (NOVE) REGISTROS SIMULTÂNEOS DE PESO E ALTURA ATÉ OS DOIS ANOS DE VIDA",
        "TER PELO MENOS 02 (DUAS) VISITAS DOMICILIARES REALIZADAS POR ACS/TACS, SENDO A PRIMEIRA ATÉ OS PRIMEIROS 30 (TRINTA) DIAS DE VIDA E A SEGUNDA ATÉ OS 06 (SEIS) MESES DE VIDA",
        "TER VACINAS CONTRA DIFTERIA, TÉTANO, COQUELUCHE, HEPATITE B, INFECÇÕES CAUSADAS POR HAEMOPHILUS INFLUENZAE TIPO B, POLIOMIELITE, SARAMPO, CAXUMBA E RUBÉOLA, PNEUMOCÓCICA, REGISTRADAS COM TODAS AS DOSES RECOMENDADAS",
        "SOMATÓRIO DAS BOAS PRÁTICAS PONTUADAS PARA CADA CRIANÇA COM ATÉ 02 (DOIS) ANOS DE VIDA DURANTE O ACOMPANHAMENTO DO DESENVOLVIMENTO INFANTIL.",
        "Nº TOTAL DE CRIANÇAS COM ATÉ 02 (DOIS) ANOS DE VIDA VINCULADAS À EQUIPE NO PERÍODO"
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
