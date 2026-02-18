import pandas as pd
from src.utils import (remover_colunas_por_nome, normalizar_codigo, renomear_coluna, remover_ultimas_linhas,
converte_numero)

def tratar_dados(df, colunas, mes):
    df = df.copy()

    df = remover_colunas_por_nome(df, colunas=colunas)

    df = remover_ultimas_linhas(df, n=2)

    df = renomear_coluna(df, "PONTUAÇÃO", mes)

    df = normalizar_codigo(df, "INE")

    df = converte_numero(df, mes)

    return df