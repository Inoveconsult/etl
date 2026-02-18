from src.pipeline_idosos import carregar_tratar_dados_idosos
from src.indicador import gerar_df_indicador

# dados = r"C:\Projetos\ETL\Dados\SM\Esf"
dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"
# # dados = r"C:\Projetos\ETL\Dados\Coroata\Esf"
# # dados = r"C:\Projetos\ETL\Dados\PRT\Esf

FAIXAS = [
        ("Regular", [(0, 25)]),
        ("Suficiente", [(25.01, 50)]),
        ("Bom", [(50.01, 75)]),
        ("Ótimo", [(75.01, 100)])
]

def gerar_df_idosos():
    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_idosos,
        indicador="C6",
        pasta_dados=dados,
        arquivos={
            "SETEMBRO" : "C6_092025.xlsx",
            "OUTUBRO"  : "C6_102025.xlsx",
            "NOVEMBRO" : "C6_112025.xlsx",
            "DEZEMBRO" : "C6_122025.xlsx",
        },
        peso=1,
        nome_nota="NOTA C6",
        faixas=FAIXAS,
        coluna_origem_excel="PONTUAÇÃO"
    )