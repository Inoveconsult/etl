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


def carregar_tratar_dados_gestantes(
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
        "TER PELO MENOS 07 (SETE) CONSULTAS PRESENCIAIS OU REMOTAS REALIZADAS POR MÉDICA(O) OU ENFERMEIRA(O) DURANTE O PERÍODO DA GESTAÇÃO.",
        "TER PELO MENOS 07 (SETE) REGISTRO DE AFERIÇÃO DE PRESSÃO ARTERIAL REALIZADOS DURANTE O PERÍODO DA GESTAÇÃO.",
        "TER PELO MENOS 07 (SETE) REGISTROS SIMULTÂNEOS DE PESO E ALTURA DURANTE O PERÍODO DA GESTAÇÃO.",
        "TER PELO MENOS 03 (TRÊS) VISITAS DOMICILIARES REALIZADAS POR ACS/TACS, APÓS A PRIMEIRA CONSULTA DO PRÉ-NATAL.",
        "TER VACINA ACELULAR CONTRA DIFTERIA, TÉTANO, COQUELUCHE (DTPA) REGISTRADA A PARTIR DA 20ª SEMANA DE CADA GESTAÇÃO.",
        "TER REGISTRO DOS TESTES RÁPIDOS OU DOS EXAMES AVALIADOS PARA SÍFILIS, HIV E HEPATITES B E C REALIZADOS NO 1º TRIMESTRE DE CADA GESTAÇÃO.",
        "TER REGISTRO DOS TESTES RÁPIDOS OU DOS EXAMES AVALIADOS PARA SÍFILIS E HIV REALIZADOS NO 3º TRIMESTRE DE CADA GESTAÇÃO.",
        "TER PELO MENOS 01 REGISTRO DE CONSULTA PRESENCIAL OU REMOTA REALIZADA POR MÉDICA(O) OU ENFERMEIRA(O) DURANTE O PUERPÉRIO.",
        "TER PELO MENOS 01 VISITA DOMICILIAR REALIZADA POR ACS/TACS DURANTE O PUERPÉRIO.",
        "TER PELO MENOS 01 ATIVIDADE EM SAÚDE BUCAL REALIZADA POR CIRURGIÃ(ÃO) DENTISTA OU TÉCNICA(O) DE SAÚDE BUCAL DURANTE O PERÍODO DA GESTAÇÃO.",
        "SOMATÓRIO DAS BOAS PRÁTICAS PONTUADAS PARA A PESSOA GESTANTE E PUÉRPERA, DURANTE CADA GESTAÇÃO",
        "Nº TOTAL DE GESTANTES E PUÉRPERAS VINCULADAS À EQUIPE NO PERÍODO."        
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
