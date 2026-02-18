from src.utils import criar_dataframe, adicionar_mes, classificar_por_faixas, calcular_nota_final, ordenar_por_nota_e_quadrimestre
from src.pipeline_m2 import tratar_dados

# dados = r"C:\Projetos\ETL\Dados\PRT\Emulti"
# dados = r"C:\Projetos\ETL\Dados\Coroata\Emulti"
# dados = r"C:\Projetos\ETL\Dados\SM\Emulti"
dados = r"C:\Projetos\ETL\Dados\Caxias\Emulti"
# dados = r"C:\Projetos\ETL\Dados\LP\Emulti"

FAIXAS = [
    ("Regular", 0, 1),
    ("Suficiente", 1.01, 2.5),
    ("Bom", 2.6, 5),
    ("Ótimo", 5.01, 100)
]

df_set = criar_dataframe(
    origem=dados +"\M2_092025.xlsx",    
    skiprows=17)    

df_out = criar_dataframe(
    origem=dados +"\M2_102025.xlsx",    
    skiprows=17)

df_nov = criar_dataframe(
    origem=dados +"\M2_112025.xlsx",    
    skiprows=17)

df_dez = criar_dataframe(
    origem=dados +"\M2_122025.xlsx",    
    skiprows=17)

colunas=["CNES","ESTABELECIMENTO","TIPO DO ESTABELECIMENTO","SIGLA DA EQUIPE",
         "NÚMERO DE AÇÕES COMPARTILHADAS REALIZADAS PELA EMULTI",
         "NÚMERO DE AÇÕES REALIZADAS PELA EMULTI"]

def gerar_df_m2():
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

    df = calcular_nota_final(df, coluna_conceito="CONCEITO", peso=4, nome_coluna_saida="NOTA M2")
    
    df = ordenar_por_nota_e_quadrimestre(df, coluna_nota="NOTA M2")

    return df