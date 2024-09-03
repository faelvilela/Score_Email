import psycopg2
import sqlite3


def consultaCob():
    
    EficazConexao = psycopg2.connect(
       user='***',
        password='***',
        host='***',
        port='***',
        database='***'
    )

    CursorConsulta = EficazConexao.cursor()
    
    query = """
        SET
        STATEMENT_TIMEOUT = '30min';
        SELECT DISTINCT
                razaosocial,
                processo,
                UPPER(email) AS email
            FROM (
                SELECT
                DEV.razaosocial,
                DEV.processo,
                UPPER(DEV.email1) AS email
            FROM
                VI_354_TB_DEVEDOR DEV
            WHERE
                DEV.email1 IS NOT NULL AND DEV.email1 <> ''
                UNION ALL
            SELECT
                DEV.razaosocial,
                DEV.processo,
                UPPER(DEV.email2) AS email
            FROM
                VI_354_TB_DEVEDOR DEV
            WHERE
                DEV.email2 IS NOT NULL AND DEV.email2 <> ''
                UNION ALL
            SELECT
                DEV.razaosocial,
                DEV.processo,
                UPPER(EMA.email) AS email
            FROM
                VI_354_TB_DEVEDOR DEV
                JOIN VI_354_TB_DEVEDOR_EMAILS EMA ON DEV.ID = EMA.IDDEVEDOR
            WHERE
                EMA.email IS NOT NULL AND EMA.email <> ''
        ) AS combined;
        """

    CursorConsulta.execute(query)
    
    linhas = CursorConsulta.fetchall()

    CursorConsulta.close()
    EficazConexao.close()
    
    # for row in linhas:
    #     print(row)
    
    print('Consulta COB OK')
    return linhas



    
def CombinaConsultas(data, conexao):
    conn = conexao
    cursor = conn.cursor()

    for row in data:
        razaosocial, processo, email = row
        cursor.execute('''           
        UPDATE usuarios
        SET razaosocial = ?, processo = ?
        WHERE email = ?
        ''', (razaosocial, processo, email))

    conn.commit()
    print('Sucesso em combinar as consultas')

