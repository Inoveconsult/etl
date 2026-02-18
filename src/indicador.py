import os
from src.utils import (
    adicionar_mes,
    classificar_por_faixas_unificada,
    calcular_nota_final,
    ordenar_por_nota_e_quadrimestre
)

FAIXAS_PADRAO = [
    ("Regular", [(None, 9.99), (70.01, None)]),
    ("Suficiente", [(10, 29.99)]),
    ("Bom", [(30, 50)]),
    ("Ótimo", [(50.01, 70)])
]


def gerar_df_indicador(
    pipeline,
    indicador,
    arquivos: dict,
    peso,
    nome_nota,
    pasta_dados,
    coluna_origem_excel,
    faixas=FAIXAS_PADRAO
):
    """
    Gera DataFrame quadrimestral para qualquer indicador.
    
    arquivos: dict -> {"SETEMBRO": "arquivo.xlsx", ...}
    """

    dfs = {}

    # =============================
    # 1️⃣ Carregar todos os meses
    # =============================
    for mes, nome_arquivo in arquivos.items():
        caminho = os.path.join(pasta_dados, nome_arquivo)

        dfs[mes] = pipeline(
            caminho_arquivo=caminho,
            faixas=faixas,
            coluna_origem_excel=coluna_origem_excel,
            coluna_pontuacao=mes
        )

    # =============================
    # 2️⃣ Unificar meses
    # =============================
    meses = list(dfs.keys())
    df = dfs[meses[0]]

    for mes in meses[1:]:
        df = adicionar_mes(df, dfs[mes], mes)

    # =============================
    # 3️⃣ Calcular resultado quadrimestral
    # =============================
    df["RESULTADO QUADRIMESTRE"] = (
        df[meses]
        .mean(axis=1)
        .round(2)
    )

    # =============================
    # 4️⃣ Classificar
    # =============================
    df["CONCEITO"] = classificar_por_faixas_unificada(
        df["RESULTADO QUADRIMESTRE"],
        faixas
    )

    # =============================
    # 5️⃣ Nota final
    # =============================
    df = calcular_nota_final(
        df=df,
        coluna_conceito="CONCEITO",
        peso=peso,
        nome_coluna_saida=nome_nota
    )

    # =============================
    # 6️⃣ Ordenar
    # =============================
    df = ordenar_por_nota_e_quadrimestre(
        df=df,
        coluna_nota=nome_nota,
        coluna_quadrimestre="RESULTADO QUADRIMESTRE"
    )

    return df
