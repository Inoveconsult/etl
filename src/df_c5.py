from src.pipeline_hipertensos import carregar_tratar_dados_hipertensos
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

def gerar_df_hipertensos():
    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_hipertensos,
        indicador="C5",
        pasta_dados=dados,
        arquivos={
            "SETEMBRO" : "C5_092025.xlsx",
            "OUTUBRO"  : "C5_102025.xlsx",
            "NOVEMBRO" : "C5_112025.xlsx",
            "DEZEMBRO" : "C5_092025.xlsx"
        },
        peso=1,
        nome_nota="NOTA C5",
        faixas=FAIXAS,
        coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR"      
    )
    