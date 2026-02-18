from src.pipeline_diabetes import carregar_tratar_dados_diabeticos
from src.indicador import gerar_df_indicador

FAIXAS = [
        ("Regular", [(0, 25)]),
        ("Suficiente", [(25.01, 50)]),
        ("Bom", [(50.01, 75)]),
        ("Ótimo", [(75.01, 100)])
]

def gerar_df_diabeticos():
    dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"

    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_diabeticos,        
        indicador="C4",
        pasta_dados=dados,
        arquivos={
            "SETEMBRO" : "C4_092025.xlsx",
            "OUTUBRO"  : "C4_102025.xlsx",
            "NOVEMBRO" : "C4_112025.xlsx",
            "DEZEMBRO" : "C4_122025.xlsx"
        },
        peso=1,
        nome_nota="NOTA C4",
        faixas=FAIXAS,        
        coluna_origem_excel="RAZÃO ENTRE O NUMERADOR E DENOMINADOR"
    )