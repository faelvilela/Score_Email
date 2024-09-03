import sqlite3, os, shutil
import pandas as pd


def exportar(conn, cursor, score):
    # Executar a consulta SQL para selecionar os usuários com score desejado
    cursor.execute('SELECT email, score, processo, razaosocial FROM usuarios WHERE score = ?', (score,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['email', 'score', 'processo', 'razaosocial'])
    nome_arquivo_excel = f'./Arquivos/Export_Score{score}.xlsx'
    # Escrever o DataFrame em uma planilha Excel
    df.to_excel(nome_arquivo_excel, index=False)

    print(f"Dados exportados para a planilha '{nome_arquivo_excel}' com sucesso!")

def apagar_todos_os_dados(cursor, conn):
    cursor.execute('DELETE FROM usuarios')
    conn.commit()
    print("Todos os dados foram apagados da tabela 'usuarios'.")
    
def imprimir(cursor, conn):
    #Exibir 50 linhas do banco
    cursor.execute('SELECT * FROM usuarios LIMIT 50')
    rows = cursor.fetchall()

    print("Dados na tabela 'usuarios':")
    for row in rows:
        print(row)

def backup(source_path, backup_path):
    try:
        if os.path.exists(source_path):
            shutil.copy2(source_path, backup_path)
            print(f"Backup do banco de dados realizado com sucesso para {backup_path}")
        else:
            print(f"O arquivo de banco de dados {source_path} não foi encontrado.")
    except Exception as e:
        print(f"Erro ao fazer backup do banco de dados: {e}")
        
        

