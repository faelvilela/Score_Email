import pandas as pd
import sqlite3
from funcoes import *
from consultaCob import *
from datetime import datetime

caminho = 'planilha/'

file_path = None
for file_name in os.listdir(caminho):
    if file_name.endswith('.xlsx'): 
        file_path = os.path.join(caminho, file_name)
        break

if not file_path:
    raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta 'planilha'.")

conn = sqlite3.connect('Q:/TI/DB/EficazDB.db')
cursor = conn.cursor()

def score5():
    sheet_name = 'Cliques por link'
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    emails = df['Contato']

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        score INTEGER NOT NULL,
        processo TEXT,
        razaosocial TEXT,
        data_inclusao TEXT,
        data_atualizacao TEXT
    )
    ''')

    for email in emails:
        email_upper = email.upper()
        data_hoje = datetime.now().strftime('%d/%m/%y %H:%M')
        
        cursor.execute('''
        INSERT INTO usuarios (email, score, data_inclusao)
        VALUES (?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
        score = excluded.score,
        data_atualizacao = ?
        ''', (email_upper, 5, data_hoje, data_hoje))

    conn.commit()
    print('Score 5 OK')

def score4():
    sheet_name = 'Visualizações'
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    emails = df['Contato']

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        score INTEGER NOT NULL,
        processo TEXT,
        razaosocial TEXT,
        data_inclusao TEXT,
        data_atualizacao TEXT
    )
    ''')

    for email in emails:
        email_upper = email.upper()
        data_hoje = datetime.now().strftime('%d/%m/%y %H:%M')
        
        cursor.execute('''
        INSERT INTO usuarios (email, score, data_inclusao)
        VALUES (?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
        score = excluded.score,
        data_atualizacao = ?
        ''', (email_upper, 4, data_hoje, data_hoje))

    conn.commit()
    print('Score 4 OK')
    
def scoreDiversos():
    sheet_name = 'Erros de recebimento'
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    ScoreMapeamentoPorMotivo = {
        'Caixa de emails está cheia': 3,
        'Erro desconhecido': 1,
        'Indisponibilidade no servidor de destino': 1,
        'Não é um contato válido': 1,
        'Ocorreu um erro temporário ao entregar a mensagem': 3,
        'Provedor classificou a mensagem como spam': 2,
        'Provedor recusou a mensagem': 1,
        'Provedor retornou que contato não existe': 1
    }

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        score INTEGER NOT NULL,
        processo TEXT,
        razaosocial TEXT,
        data_inclusao TEXT,
        data_atualizacao TEXT
    )
    ''')

    for _, row in df.iterrows():
        DescricaoMotivo = row['Descrição do motivo']
        email = row['Contato']
        score = ScoreMapeamentoPorMotivo.get(DescricaoMotivo, 1)
        email_upper = email.upper()
        data_hoje = datetime.now().strftime('%d/%m/%y %H:%M')
        
        cursor.execute('''
        INSERT INTO usuarios (email, score, data_inclusao)
        VALUES (?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
        score = excluded.score,
        data_atualizacao = ?
        ''', (email_upper, score, data_hoje, data_hoje))

    conn.commit()
    print('Score Diversos OK')

backup('Q:/TI/DB/EficazDB.db', 'Z:/TI/DB_Backup/backup_meu_banco_de_dados.db')
scoreDiversos()
score4()
score5()
data = consultaCob() #Realiza a consulta no postegreSQL (COB) e atribui para data
CombinaConsultas(data, conn) #Combina e concatena os dados dos cliente com o COB e o nosso banco SQlite
#imprimir(cursor, conn)
conn.close()

# exportar(conn, cursor, 5)
