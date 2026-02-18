from src.pipeline_mais_acesso import carregar_tratar_dados_mais_acesso
from src.indicador import gerar_df_indicador

FAIXAS = [
        ("Regular", [(None, 10), (70.01, None)]),
        ("Suficiente", [(10.01, 30)]),
        ("Bom", [(30.01, 50)]),
        ("Ótimo", [(50.01, 70)])
]

def gerar_df_mais_acesso():
    dados = r"C:\Projetos\ETL\Dados\Caxias\Esf"

    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_mais_acesso,        
        indicador="C1",
        pasta_dados=dados,
        arquivos={
            "SETEMBRO": "C1_092025.xlsx",
            "OUTUBRO" : "C1_102025.xlsx",
            "NOVEMBRO": "C1_112025.xlsx",
            "DEZEMBRO": "C1_122025.xlsx"
        },
        peso=1,
        nome_nota="NOTA C1",
        faixas=FAIXAS,        
        coluna_origem_excel="PONTUAÇÃO"
    )