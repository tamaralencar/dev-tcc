#Ler arquivos parquet gerados no ReadCSV
from ReadCSV import output_path
#import dask.dataframe as dd
import pandas as pd

pq_dimensao = f'{output_path}dimensao.parquet'
pq_ofertas_fies = f'{output_path}ofertas_fies.parquet'
pq_inscricoes =  f'{output_path}inscricoes.parquet'

df_dimensao = pd.read_parquet(pq_dimensao)
df_ofertas_fies = pd.read_parquet(pq_ofertas_fies)
df_inscricoes = pd.read_parquet(pq_inscricoes)


#oferta_groupby_ano = df_ofertas_fies.groupby(['Ano','Semestre','Vagas ofertadas Fies']).agg({'Vagas ofertadas Fies': 'sum'})
#oferta_groupby_ano = df_ofertas_fies.loc[:, ['Ano', 'Semestre', 'Vagas ofertadas Fies']].assign(soma_campo3=df_ofertas_fies['Vagas ofertadas Fies'].sum())



# Exibe o DataFrame
print(df_ofertas_fies)
#print(df_ofertas_fies.columns)




