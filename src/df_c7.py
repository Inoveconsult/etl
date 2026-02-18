from src.pipeline_saude_mulher import carregar_tratar_dados_saude_mulher
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

def gerar_df_saude_mulher():
    return gerar_df_indicador(
        pipeline=carregar_tratar_dados_saude_mulher,
        indicador="C7",
        pasta_dados=dados,
        arquivos={
            "SETEMBRO" : "C7_092025.xlsx",
            "OUTUBRO"  : "C7_102025.xlsx",
            "NOVEMBRO" : "C7_112025.xlsx",
            "DEZEMBRO" : "C7_122025.xlsx"
        },
        peso=2,
        nome_nota="NOTA C7",
        faixas=FAIXAS,
        coluna_origem_excel="SOMATÓRIO DA BOA PRÁTICA PARA CADA MULHER E HOMEM TRANSGÊNERO NA FAIXA ETÁRIA AVALIADA NA BOA PRÁTICA"
    )