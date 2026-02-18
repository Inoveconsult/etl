from src.utils import criar_dataframe, adicionar_mes, classificar_por_faixas_unificada, calcular_nota_final, ordenar_por_nota_e_quadrimestre
from src.pipeline_b1 import tratar_dados

# dados = r"C:\Projetos\ETL\Dados\PRT\Odonto"
# dados = r"C:\Projetos\ETL\Dados\Coroata\Odonto"
# dados = r"C:\Projetos\ETL\Dados\LP\Odonto"
# dados = r"C:\Projetos\ETL\Dados\SM\Odonto"
dados = r"C:\Projetos\ETL\Dados\Caxias\Odonto"

FAIXAS = [
        ("Regular", [(None, 39.99), (85, None)]),
            ("Suficiente", [(40, 59.99)]),
            ("Bom", [(60, 79.99)]),
            ("Ótimo", [(80, 84.99)])
]

df_set = criar_dataframe(
    origem=dados +"\B5_092025.xlsx",    
    skiprows=17)    

df_out = criar_dataframe(
    origem=dados +"\B5_102025.xlsx",    
    skiprows=17)

df_nov = criar_dataframe(
    origem=dados +"\B5_112025.xlsx",    
    skiprows=17)

df_dez = criar_dataframe(
    origem=dados +"\B5_122025.xlsx",    
    skiprows=17)

colunas=["CNES","ESTABELECIMENTO","TIPO DO ESTABELECIMENTO","SIGLA DA EQUIPE",
         "Nº TOTAL DE PROCEDIMENTOS ODONTOLÓGICOS PREVENTIVOS INDIVIDUAIS",         
         "Nº TOTAL DE PROCEDIMENTOS ODONTOLÓGICOS INDIVIDUAIS REALIZADOS"]

def gerar_df_b5():
    df_1 = tratar_dados(df_set, colunas, mes="SETEMBRO")

    df_2 = tratar_dados(df_out, colunas, mes="OUTUBRO")
    
    df_3 = tratar_dados(df_nov, colunas, mes="NOVEMBRO")
    
    df_4 = tratar_dados(df_dez, colunas, mes="DEZEMBRO")

    df = df_1

    df = adicionar_mes(df, df_2, "OUTUBRO")
    
    df = adicionar_mes(df, df_3, "NOVEMBRO")
    
    df = adicionar_mes(df, df_4, "DEZEMBRO")

    df["RESULTADO QUADRIMESTRE"] = df[["SETEMBRO", "OUTUBRO", "NOVEMBRO","DEZEMBRO"]].mean(axis=1).round(2)

    df["CONCEITO"] = classificar_por_faixas_unificada(df["RESULTADO QUADRIMESTRE"], FAIXAS)

    df = calcular_nota_final(df, coluna_conceito="CONCEITO", peso=1, nome_coluna_saida="NOTA B5")
    
    df = ordenar_por_nota_e_quadrimestre(df, coluna_nota="NOTA B5")

    return df
