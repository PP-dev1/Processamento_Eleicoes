from pathlib import Path
import pandas as pd

ARQUIVO = Path(__file__).parent / "dados" / "votacao.csv"

COLUNAS = [
    "SG_UF",
    "NM_MUNICIPIO",
    "NR_ZONA",
    "SG_PARTIDO",
    "DS_CARGO",
    "QT_VOTOS_NOMINAIS_VALIDOS",
]

zonas = set()
total_registros = 0

for bloco in pd.read_csv(
    ARQUIVO,
    sep=";",
    encoding="latin-1",
    usecols=COLUNAS,
    dtype="string",
    chunksize=100_000,
):
    uf = bloco["SG_UF"].fillna("").str.strip().str.upper()
    municipio = (
        bloco["NM_MUNICIPIO"].fillna("").str.strip().str.upper()
    )

    campinas = bloco.loc[
        uf.eq("SP") & municipio.eq("CAMPINAS")
    ]

    total_registros += len(campinas)

    zonas.update(
        campinas["NR_ZONA"].dropna().str.strip().unique()
    )

print(f"Registros encontrados em Campinas: {total_registros}")
print(f"Quantidade de zonas eleitorais: {len(zonas)}")
print(f"Zonas encontradas: {sorted(zonas, key=lambda z: int(z))}")