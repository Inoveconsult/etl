# from src.pipeline_criancas import carregar_tratar_dados_crianca
# from src.utils import (
#     adicionar_mes,
#     classificar_por_faixas,
#     calcular_nota_final,
#     ordenar_por_nota_e_quadrimestre
# )



# FAIXAS = [
#     ("Regular", 0, 24.99),
#     ("Suficiente", 25, 49.99),
#     ("Bom", 50, 74.99),
#     ("Ótimo", 75, 100)
# ]

# def gerar_df_crianca():
#     df = carregar_tratar_dados_crianca(
#         caminho_arquivo= dados + "/C2_Q3_2025.xlsx",
#         faixas=FAIXAS,
#         # coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
#         # coluna_pontuacao="SETEMBRO"
#     )

#     # df_out = carregar_tratar_dados_crianca(
#     #     caminho_arquivo= dados + "/C2_102025.xlsx",
#     #     faixas=FAIXAS,
#     #     coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
#     #     coluna_pontuacao="OUTUBRO"
#     # )
  
#     # df_nov = carregar_tratar_dados_crianca(
#     #     caminho_arquivo= dados + "/C2_112025.xlsx",
#     #     faixas=FAIXAS,
#     #     coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
#     #     coluna_pontuacao="NOVEMBRO"
#     # )

#     # df_dez = carregar_tratar_dados_crianca(
#     #     caminho_arquivo= dados + "/C2_122025.xlsx",
#     #     faixas=FAIXAS,
#     #     coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR",
#     #     coluna_pontuacao="DEZEMBRO"
#     # )

#     # Junta os meses
#     # df = df_set   
#     # df = adicionar_mes(df, df_out, "OUTUBRO")
#     # df = adicionar_mes(df, df_nov, "NOVEMBRO")
#     # df = adicionar_mes(df, df_dez, "DEZEMBRO")

#     df ["RESULTADO QUADRIMESTRE"] = df[["SETEMBRO","OUTUBRO","NOVEMBRO", "DEZEMBRO"]].mean(axis=1).round(2)

#     # Classifica usando função unificada
#     df["CONCEITO"] = classificar_por_faixas(
#         df["RESULTADO QUADRIMESTRE"], FAIXAS
#     )

#     df = calcular_nota_final(df=df, coluna_conceito="CONCEITO", peso=2, nome_coluna_saida="NOTA C2")

#     df = ordenar_por_nota_e_quadrimestre(
#         df,
#         coluna_nota="NOTA C2"
#     )

#     return df

from src.pipeline_criancas import carregar_tratar_dados_criancas
from src.indicador import gerar_df_indicador

# dados = r"C:\Projetos\ETL\Dados\SM\Esf"
dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"
# # dados = r"C:\Projetos\ETL\Dados\Coroata\Esf"
# # dados = r"C:\Projetos\ETL\Dados\PRT\Esf"

FAIXAS = [
    ("Regular", [(0, 25)]),
    ("Suficiente", [(25.01, 50)]),
    ("Bom", [(50.01, 75)]),
    ("Ótimo", [(75.01, 100)])
]

def gerar_df_criancas():
    caminho = dados
    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_criancas,
        indicador="C2",
        pasta_dados=caminho,
        arquivos={
            "SETEMBRO" : "C2_092025.xlsx",
            "OUTUBRO"  : "C2_102025.xlsx",
            "NOVEMBRO" : "C2_112025.xlsx",
            "DEZEMBRO" : "C2_122025.xlsx"
        },
        peso=2,
        nome_nota="NOTA C2",
        faixas=FAIXAS,        
        coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR"
    )
