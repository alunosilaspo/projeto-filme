#importar Bibliotecas
import csv
import sqlite3
import duckdb
import pandas as pd

# Conecta abrindo / criando um arquivo
conn = duckdb.connect('../dados/filme_duckdb.db')


# Criar a tabela writers (se não existir)
conn.execute('''
        CREATE TABLE IF NOT EXISTS writers(
            idwriter INTEGER  PRIMARY KEY,
            writer VARCHAR(256)
        )
''')

#Buscar os dados de
query = '''
        SELECT
            DISTINCT writers
        FROM filmes_imdb
        WHERE writers IS NOT NULL
        AND writers != ''
            '''
conn.execute(query)
rows = conn.fetchall()
# Criar um dicionário para armazenar os dados únicos de writers

writers = {}
for row in rows:
    linha = row[0].replace('[', '').replace(']', '').replace("'",'')
    if ',' in linha:
        # Dividindo a variável usando '||' como delimitador
        lista_row = linha.split(",")
        for item_lista in lista_row:
            writers[item_lista] = item_lista
    else:
        writers[linha] = linha

# Deletar os dados da tabela writers
query = 'DELETE FROM writers'
conn.execute(query)

# Inserir os dados na tabela writers
i = 1
for writers_name in writers:
    query = f'''INSERT INTO writers (idwriter,writer)
                VALUES
                ({i},'{writers_name.strip()}');'''
    conn.execute(query)
    i += 1
conn.commit()

# Fechar a conexão
conn.close()
