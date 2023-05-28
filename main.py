log_path = './entradaLog'
metadata_path = './metadado.json'
conn = None

try:
    #conexao de banco de dados
    conn = psycopg2.connect(host='localhost', port='5432', database='postgres',user='postgres', password='postgres')
    cursor = conn.cursor()
    
    # 1. Carrega o banco de dados com a tabela
    cursor.execute('drop table if exists tp_log')
    cursor.execute('create table tp_log (id integer, A integer, B integer)')
    
    df = pd.read_json(metadata_path)['INITIAL']
    for x in range(len(df['A'])):
        cursor.execute('insert into tp_log values (%s, %s, %s)', (x+1 ,df['A'][x], df['B'][x]))

    conn.commit()

    

except psycopg2.DatabaseError as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if conn is not None:
        conn.close()
        print("PostgreSQL connection is closed")
