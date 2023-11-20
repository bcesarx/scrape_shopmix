# %%
# Importar bibiotecas
print("Importando bibliotecas...")
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
###
from src import config
import time
from datetime import datetime
from tqdm import tqdm
from src import dw_utils as dw
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
import os

# %%
import platform

# %%
print(platform.system())

# %%
### atualzar lista de links no sitemap
BASE_URL = 'https://www.gruposhopmix.com/'
## obter lista de urls a partir do index

content = requests.get(config.URL_INDEX).text

### processar o html obtido 
### extracts all loc tags from content
soup = bs(content, 'html.parser')
all_locs = soup.find_all('loc')
urls = pd.DataFrame(columns = ["links_catalogo"], data=all_locs)

### remove keywords from urls
keywords = ['pagina','sao-paulo', 'todos-os-produtos', 'os-mais-vendidos', 'novidades','black-dos-campeoes']
urls = urls[~urls["links_catalogo"].str.contains('|'.join(keywords))]
## drop lines that is equal to the base url
urls = urls.loc[urls["links_catalogo"] != BASE_URL]
### obter dataset final com urls
urls.reset_index(inplace=True, drop=True)

# %%

def extract_data_from_html(soup,url):
    
    

    nome = soup.find('meta', property='og:title')['content']

    codigo = soup.find('meta', attrs={'name': 'twitter:data1'})['content']
    try:
        preco = soup.find('strong', attrs = 'preco-promocional cor-principal titulo')['data-sell-price']
    except TypeError:
        preco = "N/A"
    disponibilidade = soup.find('meta', attrs={'name': 'twitter:data2'})['content']
    descricao = soup.find('meta', attrs={'name': 'description'})['content']
    try:
        disponibilidade_entrega = soup.find('span', class_='disponibilidade disp-entrega').find('b').text
    except AttributeError:
        disponibilidade_entrega = 'Não disponível'
    try:
        estoque = soup.find('b', class_='qtde_estoque').text
    except AttributeError:
        estoque = 'Não disponível'
    imagens = [img['src'] for img in soup.find_all('img') if img.get('src') != None]
    imagens = [imagem for imagem in imagens if 'produto' in imagem]
    
    return {
        'nome': nome,
        'código': codigo,
        'preço': preco,
        'descrição': descricao,
        'disponibilidade': disponibilidade,
        'disponibilidade_entrega': disponibilidade_entrega,
        'estoque': estoque,
        'imagens': imagens
    }

# %%
# obter soup de cada url de cada produto

### filtrar os primeiros 5 registros do dataframe url

urls = urls.head(2)


df_produtos_consolidado = pd.DataFrame()
counter = 0
print("iniciando_loop")
for link in tqdm(urls["links_catalogo"], desc='Obtendo dados dos produtos', unit='produto'):

    ## baixar pagina do produto
    prod_content= requests.get(link).text
    
    ### converter página em soup
    prod_content_soup = bs(prod_content, 'html.parser')
    ### extrair dicionário com os dados chave do produto ## converter dicionário em dataframe
    try:
        dict_produto = extract_data_from_html(prod_content_soup,link)
    except exception as e:
        print(e, "___>" ,'Erro ao obter dados do produto: --->',link)
        continue
    ## converter a lita de imagens em str
    dict_produto["imagens"] = str(dict_produto["imagens"])

    ##criar um dataframe do produto
    df_produto = pd.DataFrame(dict_produto, index=[0])
    ###inserir o link do produto
    df_produto["link"] = link
    
    ### inseirr a data de extração
    df_produto["data_extracao"] = datetime.now()
    ## adicionar ao dataframe consolidado
    df_produtos_consolidado = pd.concat([df_produtos_consolidado, df_produto], ignore_index=True)
    ##colocar uma pausa de 1 segundo a cada 50 produtos
    counter += 1
    time.sleep(5)
    if counter  == 100:
        time.sleep(57)
        # break
        counter = 0
    

# salvar dataframe consolidado em csv

#df_produtos_consolidado.to_csv('data/produtos_consolidado.csv', index=False, sep='|')

### salvar dataframe consolidado no banco de dados



# %%
### carregar no banco


if platform.system() == 'Windows':
    p_host = 'localhost'
    p_port = 5432
    db = 'postgres'
    ssh = True
    ssh_user = 'ubuntu'
    ssh_host = '144.22.150.9'
    psql_user = 'postgres'


    psql_pass = 'alice11'
    ssh_pkey = r"C:\Users\bcesa\OneDrive\Documentos\Infra na Núvem\mydata\ssh-key-2022-10-28.key"

    pgres = dw.Postgresql_connect(pgres_host=p_host, pgres_port=p_port, db=db, ssh=ssh, ssh_user=ssh_user, ssh_host=ssh_host, ssh_pkey=ssh_pkey, psql_user=psql_user
                                , psql_pass=psql_pass)
    #initiates a connection to the PostgreSQL database. In this instance we use ssh and must specify our ssh credentials.

    #You'll need to define psql_user and psql_pass using input() and getpass() to temporarily store your credentials.
    #Alternatively, best practice you may be to store your credentials as environment variables.
    # psql_user = input("Please enter your database username:")


elif platform.system() == 'Linux':
    print("Linux")
    ### checa se o arquivo da chave existe no diretório atual
    if os.path.isfile('ssh-key-2022-10-28.key'):
        print("Arquivo da chave existe")
    
    p_host = 'localhost'
    p_port = 5432
    db = 'postgres'
    ssh = True
    ssh_user = 'ubuntu'
    ssh_host = '144.22.150.9'
    psql_user = 'postgres'
    
    psql_pass = 'alice11'
    ssh_pkey = 'ssh-key-2022-10-28.key'
    
    pgres = dw.Postgresql_connect(pgres_host=p_host, pgres_port=p_port, db=db, ssh=ssh, ssh_user=ssh_user, ssh_host=ssh_host, ssh_pkey=ssh_pkey, psql_user=psql_user
                                , psql_pass=psql_pass)

# %%
#df_produtos_consolidado = pd.read_csv('data/produtos_consolidado.csv', sep='|')

# %%
### criar conexão com o banco de dados
## atualizar tabela

pgres.replace_table(df_produtos_consolidado, 'catalogo_shopmix')

# %%


# %%


# sql_statement = """
# SELECT *
# FROM catalogo_shopmix
# ;
# """
# query_df = pgres.query( query=sql_statement)
# query_df
# #returns the results of an sql statement as a pandas dataframe. 
# #This example returns the column names and data types of table 'ey_test_table'.

# %%



