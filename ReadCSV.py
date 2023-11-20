import pandas as pd
import csv
import pyarrow as pw
import numpy as np
import os


file_path = 'C:\\Users\\tamara.alencar\\PycharmProjects\\code-tcc\\datasets\\'
output_path = 'C:\\Users\\tamara.alencar\\PycharmProjects\\code-tcc\\outputs\\'


#Arquivos Orfetas FIES de 2019-2022
sem2_2019 = f'{file_path}relatorio_dados_abertos_oferta_22019_18102021.csv'
sem1_2019 = f'{file_path}relatorio_dados_abertos_oferta_12019_18102021.csv'
sem1_2020 = f'{file_path}relatorio_dados_abertos_oferta_12020_18102021.csv'
sem2_2020 = f'{file_path}relatorio_dados_abertos_oferta_22020_18102021.csv'
sem1_2021 = f'{file_path}relatorio_dados_abertos_oferta_12021_18102021.csv'
sem2_2021 = f'{file_path}relatorio_dados_abertos_oferta_22021_18102021.csv'
sem1_2022 = f'{file_path}relatorio_dados_abertos_oferta_12022_15072022.csv'

list_files = [sem2_2019,sem1_2019,sem1_2020,sem2_2020,sem1_2021,sem2_2021,sem1_2022]

#Le os datasets Ofertas FIES
list_ofertas_fies = []

for i in list_files:
    with open(i,'r',encoding='latin') as file:
        csv_reader = csv.reader(file, delimiter=';')
        header = next(csv_reader)
        list_ofertas_fies.append(header)
        
        for row in csv_reader:
            list_ofertas_fies.append(row)

df_ofertas_fies = pd.DataFrame(list_ofertas_fies[1:], columns=list_ofertas_fies[0])
ft_select_cols = ["Ano","Semestre","Nome Mantenedora","Código e-MEC da Mantenedora","CNPJ da mantenedora","Nome da IES","Código e-MEC da IES",
"Organização Acadêmica da IES","UF da IES","Município da IES","Nome do Local de oferta","Área do conhecimento","Subárea do conhecimento",
"Código do Curso","Nome do Curso","Turno","Grau","Vagas autorizadas e-mec","Vagas ofertadas FIES","Vagas além da Oferta","Vagas ocupadas",
"Participa do P-FIES","Vagas Ofertadas P-FIES"," Valor bruto do curso "]

df_ofertas_fies = df_ofertas_fies[ft_select_cols]
df_ofertas_fies.rename(columns={'Código e-MEC da IES':'CODIGO_IES'}, inplace=True) #renomeia o campo de código da IES
oferta_manter = df_ofertas_fies['CODIGO_IES'] != 'Código e-MEC da IES'
df_oferta_filtrado = df_ofertas_fies[oferta_manter]

df_oferta_filtrado2 = df_oferta_filtrado[df_oferta_filtrado['CODIGO_IES']!=''].copy()
df_oferta_filtrado2['CODIGO_IES'] = df_oferta_filtrado2['CODIGO_IES'].astype('int64')


#Datasets Inscrições
list_inscricoes = []
for files in os.listdir(file_path):
    if files.startswith('relatorio_inscricao_dados_abertos_fies_') and files.endswith('.csv'):
        arq_insc = os.path.join(file_path, files)
        with open(arq_insc,'r',encoding='latin') as file:        
            df_insc = pd.read_csv(file, delimiter=';')
            list_inscricoes.append(df_insc)

# Concatena os DataFrames da lista em um único DataFrame
df_inscricoes = pd.concat(list_inscricoes, ignore_index=True)
insc_select =['Ano do processo seletivo','Semestre do processo seletivo','Sexo','UF de residência','Municipio de residência','Etnia/Cor',
'Pessoa com deficiência?','Nº de membros Grupo Familiar','Renda familiar mensal bruta','Renda mensal bruta per capita','Área do conhecimento',
'Opções de cursos da inscrição','Nome mantenedora','Natureza Jurídica Mantenedora','CNPJ da mantenedora','Código e-MEC da Mantenedora',
'Nome da IES','Código e-MEC da IES','Organização Acadêmica da IES','Município da IES','UF da IES','Nome do Local de oferta',
'Munícipio do Local de Oferta','UF do Local de Oferta','Código do curso','Nome do curso','Turno','Situação Inscrição Fies',
'Percentual de financiamento','Semestre do financiamento','Qtde semestre financiado'] 

df_inscricoes = df_inscricoes[insc_select]
df_inscricoes = df_inscricoes[df_inscricoes['Situação Inscrição Fies']!= 'PARTICIPACAO CANCELADA PELO CANDIDATO']

#Datasets Dimensões
dm_cursos = f'{file_path}PDA_Dados_Cursos_Graduacao_Brasil.csv'
dm_instituicoes = f'{file_path}PDA_Lista_Instituicoes_Ensino_Superior_do_Brasil_EMEC.csv'

df_cursos = pd.read_csv(dm_cursos)
df_instituicoes = pd.read_csv(dm_instituicoes)

df_instituicoes.rename(columns={'CODIGO_DA_IES':'CODIGO_IES', 'MUNICIPIO':'MUNICIPIO_df2', 'UF':'UF_df2',
                                'ORGANIZACAO_ACADEMICA':'ORGANIZACAO_ACADEMICA_df2'}, inplace=True) #renomeia o campo de código da IES

dm_merge = pd.merge(df_instituicoes,df_cursos, on="CODIGO_IES", how='left')
dm_select_cols = ["CODIGO_IES","NOME_IES","SIGLA","CATEGORIA_DA_IES","SITUACAO_IES","CATEGORIA_ADMINISTRATIVA","ORGANIZACAO_ACADEMICA",
               "CODIGO_CURSO","NOME_CURSO","GRAU","AREA_OCDE","MODALIDADE","SITUACAO_CURSO","QT_VAGAS_AUTORIZADAS","CARGA_HORARIA",
               "CODIGO_AREA_OCDE_CINE","AREA_OCDE_CINE","MUNICIPIO","UF","REGIAO"]
dm_select = dm_merge[dm_select_cols]
df_dimensao = dm_select[dm_select["SITUACAO_IES"] =="Ativa"]


#Gera arquivos parquet
#df_oferta_filtrado2.to_parquet(f'{output_path}ofertas_fies.parquet')
#df_dimensao.to_parquet(f'{output_path}dimensao.parquet')
#df_inscricoes.to_parquet(f'{output_path}inscricoes.parquet')

