# from src.pipeline_gestante import carregar_tratar_dados_gestante
# from src.utils import (
#     adicionar_mes,
#     calcular_nota_final,
#     ordenar_por_nota_e_quadrimestre,
#     classificar_por_faixas
# )

# dados = r"C:\Projetos\ETL\Dados\SM\Esf"
# # dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"
# # dados = r"C:\Projetos\ETL\Dados\Coroata\Esf"
# # dados = r"C:\Projetos\ETL\Dados\PRT\Esf"

# FAIXAS = [
#     ("Regular", 0, 25),
#     ("Suficiente", 25.01, 50),
#     ("Bom", 50.01, 75),
#     ("Ótimo", 75.01, 100)
# ]

# def gerar_df_gestante_puepera():

#     df = carregar_tratar_dados_gestante(
#             caminho_arquivo= dados + "/C3_Q3_2025.xlsx",
#             faixas=FAIXAS,
#         #     coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
#         #     coluna_pontuacao="SETEMBRO"
#     )

# #     df_out = carregar_tratar_dados_gestante(
# #             caminho_arquivo= dados + "/C3_102025.xlsx",
# #             faixas=FAIXAS,
# #             coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
# #             coluna_pontuacao="OUTUBRO"
# #     )
 
# #     df_nov = carregar_tratar_dados_gestante(
# #             caminho_arquivo= dados + "/C3_112025.xlsx",
# #             faixas=FAIXAS,
# #             coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
# #             coluna_pontuacao="NOVEMBRO"
# #     )

# #     df_dez = carregar_tratar_dados_gestante(
# #             caminho_arquivo= dados + "/C3_122025.xlsx",
# #             faixas=FAIXAS,
# #             coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
# #             coluna_pontuacao="DEZEMBRO"
# #     )

#      # Junta os meses
# #     df = df_set   
# #     df = adicionar_mes(df, df_out, "OUTUBRO")
# #     df = adicionar_mes(df, df_nov, "NOVEMBRO")
# #     df = adicionar_mes(df, df_dez, "DEZEMBRO")

#     df ["RESULTADO QUADRIMESTRE"] = df[["SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO"]].mean(axis=1).round(2)

#     df["CONCEITO"] = classificar_por_faixas(df["RESULTADO QUADRIMESTRE"], FAIXAS)

#     df = calcular_nota_final(df=df, coluna_conceito="CONCEITO", peso=2, nome_coluna_saida="NOTA C3")

#     df = ordenar_por_nota_e_quadrimestre(df=df, coluna_nota="NOTA C3")

#     return df

from src.pipeline_gestante import carregar_tratar_dados_gestantes
from src.indicador import gerar_df_indicador

FAIXAS = [
    ("Regular", [(0, 25)]),
    ("Suficiente", [(25.01, 50)]),
    ("Bom", [(50.01, 75)]),
    ("Ótimo", [(75.01, 100)])
]

dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"

def gerar_df_gestante_puerpera():
    caminho = dados
    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_gestantes,
        indicador="C3",
        pasta_dados=caminho,
        arquivos={
            "SETEMBRO" : "C3_092025.xlsx",
            "OUTUBRO"  : "C3_102025.xlsx",
            "NOVEMBRO" : "C3_112025.xlsx",
            "DEZEMBRO" : "C3_122025.xlsx"
        },
        peso=2,
        nome_nota="NOTA C3",
        faixas=FAIXAS,
        coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR"
    )