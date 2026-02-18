from src.utils import criar_dataframe, adicionar_mes, classificar_por_faixas, calcular_nota_final, ordenar_por_nota_e_quadrimestre
from src.pipeline_b1 import tratar_dados

# dados = r"C:\Projetos\ETL\Dados\PRT\Odonto"
# dados = r"C:\Projetos\ETL\Dados\Coroata\Odonto"
# dados = r"C:\Projetos\ETL\Dados\LP\Odonto"
# dados = r"C:\Projetos\ETL\Dados\SM\Odonto"
dados = r"C:\Projetos\ETL\Dados\Caxias\Odonto"

FAIXAS = [
    ("Regular", 0, 2.99),
    ("Suficiente", 3, 5.99),
    ("Bom", 6, 7.99),
    ("Ótimo", 8, 100)
]

df_set = criar_dataframe(
    origem=dados +"\B6_092025.xlsx",    
    skiprows=17)    

df_out = criar_dataframe(
    origem=dados +"\B6_102025.xlsx",    
    skiprows=17)

df_nov = criar_dataframe(
    origem=dados +"\B6_112025.xlsx",    
    skiprows=17)

df_dez = criar_dataframe(
    origem=dados +"\B6_122025.xlsx",    
    skiprows=17)

colunas=["CNES","ESTABELECIMENTO","TIPO DO ESTABELECIMENTO","SIGLA DA EQUIPE",
         "Nº TOTAL DE PROCEDIMENTOS 'TRATAMENTO RESTAURADOR ATRAUMÁTICO' REALIZADOS PELA ESB",
         "Nº TOTAL DE PROCEDIMENTOS RESTAURADORES REALIZADOS PELA ESB"]

def gerar_df_b6():
    df_1 = tratar_dados(df_set, colunas, mes="SETEMBRO")

    df_2 = tratar_dados(df_out, colunas, mes="OUTUBRO")
    
    df_3 = tratar_dados(df_nov, colunas, mes="NOVEMBRO")
    
    df_4 = tratar_dados(df_dez, colunas, mes="DEZEMBRO")

    df = df_1

    df = adicionar_mes(df, df_2, "OUTUBRO")

    df = adicionar_mes(df, df_3, "NOVEMBRO")
    
    df = adicionar_mes(df, df_4, "DEZEMBRO")

    df["RESULTADO QUADRIMESTRE"] = df[["SETEMBRO", "OUTUBRO", "NOVEMBRO","DEZEMBRO"]].mean(axis=1).round(2)

    df["CONCEITO"] = classificar_por_faixas(df["RESULTADO QUADRIMESTRE"], FAIXAS)

    df = calcular_nota_final(df, coluna_conceito="CONCEITO", peso=1, nome_coluna_saida="NOTA B6")
    
    df = ordenar_por_nota_e_quadrimestre(df, coluna_nota="NOTA B6")

    return df
