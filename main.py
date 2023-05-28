import psycopg2
import pandas as pd

def read_log(entradaLog):  # sourcery skip: hoist-statement-from-if, identity-comprehension, merge-duplicate-blocks, merge-else-if-into-elif, swap-if-else-branches
    df = pd.read_csv(entradaLog, sep='<', names=['LOG'], engine='python') ## leitura do log
    dados = [x for x in df.LOG.str.strip('>')]
    lista_ckpt , lista_commit ,operations_list , starts_list = [],[],[],[]
    index_ckpt = -1

    for n, x in enumerate(dados):
        if x.startswith('start'): # Verifica transações startadas 
            if n > index_ckpt:
                starts_list.append(x.lstrip('start '))
        elif x.startswith('commit'): # Verifica existência de transações commitadas
            if n > index_ckpt:
                lista_commit.append(x.lstrip('commit '))
            else: # Caso, não tenha CKPT refaz todas as transações commitadas
                lista_commit.append(x.lstrip('commit '))

    transaction_list = check_transactions((lista_ckpt + starts_list), lista_commit) # verifica a transdação da lista
    
    for x in dados:
        if x.startswith('T'): # Verifica se a operação pertence à uma transação commitada na aplicação
            operation = list(map(str, x.replace('(','').replace(')','').split(',')))
            if operation[0] in transaction_list:
                operations_list.append(operation)
    return operations_list


def check_transactions(check, commit):
    verificar_transactions = []
    for x in check:
        if x in commit:
            print(f'Transação {x} realizou REDO.')
            verificar_transactions.append(x)
        else:
            print(f'Transação {x} realizou UNDO.')
    print()
    return verificar_transactions

#----------------------------------------------- Initial code source -------------------------------------

entradaLog = './teste'
metadata_path = './metadado.json'
conn = None

try:
    #conexao de banco de dados
    conn = psycopg2.connect(host='localhost', port='5432', database='postgres',user='postgres', password='password')
    cursor = conn.cursor()
    
    # Carrega o banco de dados com a tabela
    cursor.execute('drop table if exists vintage_log')
    cursor.execute('create table vintage_log (id integer, A integer, B integer)')
    
    df = pd.read_json(metadata_path)['INITIAL']
    for x in range(len(df['A'])):
        cursor.execute('insert into vintage_log values (%s, %s, %s)', (x+1 ,df['A'][x], df['B'][x]))

    conn.commit()

    update_operations = read_log(entradaLog) # Lendo o log de entrada e verificando quais transações precisam fazer REDO/undo  
    
    # verifica valores da lista e atualizar no banco
    for op in update_operations:
        cursor.execute(f'select {op[2]} from vintage_log where id = {op[1]}')
        if int(cursor.fetchone()[0]) != int(op[4]): ## AKI COMPARA VALORES A ATUALIZAR
            cursor.execute(f'update vintage_log set {op[2]} = {op[4]} where id = {op[1]}')
            #print(f'Transação {op[0]} atualizou: id = {op[1]}, coluna = {op[2]}, valor = {op[4]}.')     
    conn.commit()
    
    # imprimi os metadados 
    cursor.execute('select * from vintage_log order by id')
    row = cursor.fetchall()
    json = {"TABLE":{}}
    json["TABLE"]["A"] = [x[1] for x in row]
    json["TABLE"]["B"] = [x[2] for x in row]
    print('\nDados após REDO:\n',json)
    cursor.close()

    
except psycopg2.DatabaseError as error:
    print()
    print("Error while connecting to PostgreSQL\n", error)

finally:
    if conn is not None:
        conn.close()
        print()
        print("PostgreSQL connection is closed\n")
