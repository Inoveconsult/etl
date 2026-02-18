import pandas as pd

def converte_numero(df, coluna):
    """
    Converte coluna numérica com vírgula decimal para float.
    
    Exemplo: '13,58' -> 13.58
    Valores inválidos viram NaN.
    """
    df[coluna] = (
        pd.to_numeric(
            df[coluna]
            .astype(str)
            .str.replace(",", ".", regex=False),
            errors="coerce"
        )
    )
    return df