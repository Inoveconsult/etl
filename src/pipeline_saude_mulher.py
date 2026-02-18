# import pandas as pd
# from src.utils import (
#     converte_numero,
#     normalizar_codigo,        
#     renomear_meses,
#     remover_ultimas_linhas
# )


# def carregar_tratar_dados_saude_mulher(
#     caminho_arquivo,
#     faixas,
#     skiprows=16
# ):
#     """
#     Pipeline completo de tratamento do arquivo
#     Desenvolvimento Infantil.

#     Etapas:
#     - Leitura do Excel
#     - Remoção de colunas desnecessárias
#     - Remoção de colunas por índice
#     - Remoção de linhas finais
#     - Renomeação de coluna
#     - Normalização de pontuação
#     - Classificação por faixas
#     - Normalização do código INE
#     """

#     df = pd.read_excel(caminho_arquivo, skiprows=skiprows)

#     # remover colunas fixas
#     df = df.drop(
#         columns=[
#             "INDICADOR",
#             "CNES",
#             "ESTABELECIMENTO",
#             "TIPO DO ESTABELECIMENTO",
#             "SIGLA DA EQUIPE"
#         ],
#         errors="ignore"
#     )

#     # remover linhas desnecessárias
#     df = remover_ultimas_linhas(df, n=2)

#     # renomear coluna de pontuação
#     df = renomear_meses(df)

#     # normalizar pontuação
#     for col in df.columns:
#         if col in ["SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"]:
#             df = converte_numero(df, col)
    
#     # normalizar INE
#     df = normalizar_codigo(df, "INE")

#     return df   

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


def carregar_tratar_dados_saude_mulher(
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
        "TER PELO MENOS 01 (UM) EXAME DE RASTREAMENTO PARA CÂNCER DO COLO DO ÚTERO EM MULHERES E EM HOMENS TRANSGÊNERO DE 25 A 64 ANOS DE IDADE, COLETADO, SOLICITADO OU AVALIADO NOS ÚLTIMOS 36 MESES",
        "TOTAL DE MULHER E HOMEM TRANSGÊNERO ENTRE 25 E 64 ANOS",
        "NM.A/DN.A",        
        "TER PELO MENOS 01 (UMA) DOSE DA VACINA HPV PARA CRIANÇAS E ADOLESCENTES DO SEXO FEMININO DE 09 A 14 ANOS DE IDADE",
        "TOTAL DE CRIANÇAS E ADOLESCENTES DO SEXO FEMININO DE 09 A 14 ANOS",
        "NM.B/DN.B",
        "TER PELO 01 (UM) ATENDIMENTO PRESENCIAL OU REMOTO, PARA ADOLESCENTES, MULHERES E HOMENS TRANSGÊNERO DE 14 A 69 ANOS DE IDADE, SOBRE ATENÇÃO À SAÚDE SEXUAL E REPRODUTIVA, REALIZADO NOS ÚLTIMOS 12 MESES",
        "TOTAL DE MULHER E HOMEM TRANSGÊNERO DE 14 A 69 ANOS",
        "NM.C/DN.C",
        "TER REGISTRO DE PELO MENOS 01 (UM) EXAME DE RASTREAMENTO PARA CÂNCER DE MAMA EM MULHERES DE 50 A 69 ANOS DE IDADES, SOLICITADO OU AVALIADO NOS ÚLTIMOS 24 MESES",
        "TOTAL DE MULHER E HOMEM TRANSGÊNERO DE 50 A 69 ANOS",
        "NM.D/DN.D"     
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

