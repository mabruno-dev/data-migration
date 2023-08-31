import pydoc
import datetime
from decimal import Decimal
import funcoes as fn
from os import path

def printLog(text,showdt=True):   
    try:
        datetimeLog = datetime.datetime.today().strftime("%d/%m/%Y %H:%M:%S")
        dateLog = datetime.datetime.today().strftime("%d_%m_%Y")
        
        arq = open(f'Log_{dateLog}.txt','a')
        
        if showdt:
            arq.write(f'\n{str(text)} | {datetimeLog}\n')
        else:
            arq.write(f'\n{str(text)}\n')
        
        arq.close()
        if showdt:
            print(f'{text} | {datetimeLog}\n')
        else:
            print(f'{text}\n')
    except Exception as E:
        print(f'Erro ao gravar log: {E.__str__()}')

#This function will read all the data from the databases 
def reading_onboardold(table_name):
    try:
        dataQuery = fn.ler_ini('QUERY_PARAMS','SOURCE_DATE')
        clientId = fn.ler_ini('QUERY_PARAMS','CLIENT_ID')
        if clientId.find(',') > 0:
            clientId = tuple(clientId.split(','))

        conn_str = f"""DRIVER={'{SQL Server}'};
        SERVER={fn.ler_ini('CONNECTION_SOURCE','HOST')};
        DATABASE={fn.ler_ini('CONNECTION_SOURCE','DATABASE')};
        UID={fn.ler_ini('CONNECTION_SOURCE','USERNAME')};
        PWD={fn.ler_ini('CONNECTION_SOURCE','PASSWORD')};
        Trusted_Connection=no;"""

        conn = pyodbc.connect(conn_str,timeout = 3)

        cursor = conn.cursor()

        printLog(f'Capturando dados da {table_name}...',True)

        if table_name == 'MVE': 
            cursor.execute(f"""SELECT 
                * 
                FROM 
                {table_name} 
                WHERE CAST(DT_CAIXA_NEW AS DATE) >= '{dataQuery}' AND CLIENTE_TOTAL {f'IN {clientId}' if isinstance(clientId,tuple) else f'= {clientId}'}""")
        
        elif table_name == 'MFP':
            cursor.execute(f"""SELECT 
                MFP.*
                FROM 
                MFP
                INNER JOIN MVE ON MVE.MOVIMENTO = MFP.MOVIMENTO
                    AND MVE.CLIENTE_TOTAL = MFP.CLIENTE
                    AND CAST(MVE.DT_CAIXA_NEW AS DATE) >= '{dataQuery}' AND MFP.CLIENTE {f'IN {clientId}' if isinstance(clientId,tuple) else f'= {clientId}'}""")
        
        elif table_name == 'IME':
            cursor.execute(f"""SELECT 
            IME.*
            FROM 
            IME
            INNER JOIN MVE ON MVE.MOVIMENTO = IME.MOVIMENTO
                AND MVE.CLIENTE_TOTAL = IME.CLIENTE_TOTAL
                AND CAST(MVE.DT_CAIXA_NEW AS DATE) >= '{dataQuery}' AND IME.CLIENTE_TOTAL {f'IN {clientId}' if isinstance(clientId,tuple) else f'= {clientId}'}""")

        else:
            conn.close()
            printLog(f'Não existem dados a serem capturados.')
            return None

        values_from_table = []
        for row in cursor:
            values_from_table.append(row)
            #print(f'MOVIMENTO: {row(0)} | {dt.strftime()}')
        values_from_each_table = tuple(values_from_table)

        conn.close()

        printLog(f'Existem {len(values_from_each_table)} dados da tabela {table_name} a serem migrados.')

        return values_from_each_table
    except Exception as E:
        printLog(f'Erro na "reading_onboardold": {str(E)}')
        input('Pressione qualquer tecla para encerrar...')
        exit(0)        

#Here I'll change the tuple to put all elements in the right sintaxe
def changing_reading_onboaradold_tuple(table_name):
    archives = reading_onboardold(table_name)
    if archives: 
        try:
            list(archives)
            for row in archives:
                for element in range(len(row)):
                    if isinstance(row[element], datetime.datetime):
                        dt = row[element]
                        formated_datetime = dt.strftime('%Y-%m-%d %H:%M:%S')
                        row[element] = formated_datetime

                    elif isinstance(row[element], Decimal):
                        row[element] = float(row[element])

            return tuple(archives)

        except Exception as E:
            printLog(f'Erro na "changing_reading_onboard_tuple": {str(E)}')
            input('Pessione qualquer tecla para sair...')
            exit(0)

# #Here I'll call the "reading function" to get all the values from the tables and add to tables
def adding_to_onboard(table_name):
    archives = changing_reading_onboaradold_tuple(table_name)
    """DRIVER={SQL Server};
    SERVER=SERVIDOR;
    DATABASE=onboard;
    UID=inffel;
    PWD=*inFF3l&2016;
    Trusted_Connection=no;"""

    conn_str = f"""DRIVER={'{SQL Server}'};
    SERVER={fn.ler_ini('CONNECTION_TARGET','HOST')};
    DATABASE={fn.ler_ini('CONNECTION_TARGET','DATABASE')};
    UID={fn.ler_ini('CONNECTION_TARGET','USERNAME')};
    PWD={fn.ler_ini('CONNECTION_TARGET','PASSWORD')};
    Trusted_Connection=no;"""

    conn = pyodbc.connect(conn_str,timeout = 3)

    cursor = conn.cursor()
    params_MVE = '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    params_IME = '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    params_MFP = '(?,?,?,?,?,?)'

    printLog('\nBanco alvo conectado!',True)

    try:
        for elements in archives:
            query_sql = f"""INSERT INTO {table_name}
            VALUES"""
            
            if table_name == 'MVE':
                printLog('Inserindo dados na MVE alvo...')
                movement = elements[0]
                clientId = elements[1]
                if checking_ifData_already_exists(table_name,movement,clientId,cursor):
                    printLog(f"MVE(OK) > Registro: {movement} - Cliente: {clientId}")
                    pass
                else:
                    query_sql = query_sql + params_MVE
                    cursor.execute(query_sql,elements)
                    cursor.commit()
                    printLog(f"MVE(New) > Movimento: {movement} - Cliente: {clientId}")
            
            elif table_name == 'IME':
                printLog('Inserindo dados na IME alvo...')
                record = elements[3]
                clientId = elements[2]
                if checking_ifData_already_exists(table_name,record,clientId,cursor):
                    printLog(f"IME(OK) > Registro: {record} - Cliente: {clientId}")
                    pass
                else:
                    query_sql = query_sql + params_IME
                    cursor.execute(query_sql,elements)
                    cursor.commit()
                    printLog(f"IME(New) > Registro: {record} - Cliente: {clientId}")
            
            elif table_name == 'MFP':
                printLog('Inserindo dados na MFP alvo...')
                record = elements[2]
                clientId = elements[1]
                if checking_ifData_already_exists(table_name,record,clientId,cursor):
                    printLog(f"MFP(OK) > Registro: {record} - Cliente: {clientId}")
                    pass
                else:
                    query_sql = query_sql + params_MFP
                    cursor.execute(query_sql,elements)
                    cursor.commit()
                    printLog(f"MFP(New) > Registro: {record} - Cliente: {clientId}")

    except Exception as E:
        printLog(f'Erro na "adding_to_onboard": {str(E)}')
        input('Pressione qualquer tecla para sair...')
        exit(0)

    conn.close()

#Getting all the tables names from onboardold(same as Onboard) to use as parameters to the function "adding_to_onboard"
def getting_table_names():

    try:
        conn_str = f"""DRIVER={'{SQL Server}'};
        SERVER={fn.ler_ini('CONNECTION_SOURCE','HOST')};
        DATABASE={fn.ler_ini('CONNECTION_SOURCE','DATABASE')};
        UID={fn.ler_ini('CONNECTION_SOURCE','USERNAME')};
        PWD={fn.ler_ini('CONNECTION_SOURCE','PASSWORD')};
        Trusted_Connection=no;"""

        conn = pyodbc.connect(conn_str,timeout = 3)
        cursor = conn.cursor()

        printLog('Banco origem conectado!',True)

        query = ("""SELECT name
        FROM sys.tables""")

        cursor.execute(query)

        list_names_tables = []
        for name in cursor:
            if name[0] == 'MFP':
                list_names_tables.append(name[0])
            if name[0] == 'IME':
                list_names_tables.append(name[0])
            if name[0] == 'MVE':
                list_names_tables.append(name[0])

        tuple_names_tables = tuple(list_names_tables)

        conn.close()
        
        printLog(f'Tabelas {tuple_names_tables} capturadas!',True)

        return tuple_names_tables
    
    except Exception as E:
        printLog(f'Erro na "getting_table_names": {str(E)}')
        input('Pressione qualquer tecla para encerrar...')
        exit(0)

#checking if data already exists
def checking_ifData_already_exists(table_name,regId,clientId,cursor):
    
    try:
        if table_name == 'MVE':
            cursor.execute(f'Select MOVIMENTO from MVE where MOVIMENTO = {regId} and CLIENTE_TOTAL = {clientId}')
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
            
        elif table_name == 'IME':
            cursor.execute(f'Select REGISTRO from IME where REGISTRO = {regId} and CLIENTE_TOTAL = {clientId}')
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
        
        elif table_name == 'MFP':
            cursor.execute(f'Select REGISTRO from MFP where REGISTRO = {regId} and CLIENTE = {clientId}')
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
    except Exception as E:
        printLog(f'Erro na "checking_ifData_already_exists": {str(E)}')
        input('Pressione qualquer tecla para encerrar...')
        exit(0)

#main
def main():
    printLog('\n------------ Início da execução')
    if not path.isfile('config.ini'):
        fn.criar_ini()
        printLog('Arquivo INI criado!')
    
    fn.criar_par_ini('QUERY_PARAMS','CLIENT_ID','')

    table_names = getting_table_names()
    for values in table_names:
        adding_to_onboard(values)
    printLog('\n------------ Fim da execução')
    

main()
