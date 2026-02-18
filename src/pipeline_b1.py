import pandas as pd
from src.utils import (renomear_coluna, remover_colunas_por_nome, 
                   remover_ultimas_linhas, normalizar_codigo, converte_numero)


def tratar_dados(df, colunas, mes):
    df = df.copy()
    
    df = remover_colunas_por_nome(df, colunas=colunas)
    
    df = renomear_coluna(df, "RAZÃO ENTRE O NUMERADOR E DENOMINADOR MULTIPLICADO POR 100", mes)
    
    df = normalizar_codigo(df, "INE")

    df = converte_numero(df, mes)

    df = remover_ultimas_linhas(df, n=2)

    return df


