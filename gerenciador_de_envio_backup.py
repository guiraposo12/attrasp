import boto3
import os
import pandas as pd
import mysql.connector
from datetime import datetime,timezone
import time

tabela_repair = 'repair'

class Gerenciador_de_envio:
    def __init__(self,access_key,secret_access_key):
        self.access_key = access_key 
        self.secret_access_key = secret_access_key
        self.bucket='datalake-dev-landing'
        self.db_config = {
            "host": "54.232.255.210",
            "user": "root",
            "password": "EFD@puc2023",
            "database": "jiga"
        }
        self.db_config_repair = {
            "host": "54.232.255.210",
            "user": "root",
            "password": "EFD@puc2023",
            "database": "db_puc"
        } 
    
    def test_connection(self):
        try:
        # Establish connection to MySQL database
            connection = mysql.connector.connect(**self.db_config)

            if connection.is_connected():
                # Execute a simple query to test the connection
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                connection.close()
                return True
        except Exception as e:
            print("Error while connecting to MySQL:", e)
            return False

    def get_protocol_index_and_sn_list(self):
        connection = mysql.connector.connect(**self.db_config_repair)
        cursor = connection.cursor()
        if connection:
            query = f"select protocol,sn,repair_state_id from {tabela_repair} where protocol like '%JIG1%'"
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            protocol_index = [int(result[0][-6:]) for result in results]
            sn_list = [result [1] for result in results if result[2] == 1]
            if len(protocol_index) == 0:
                return 0,[]
            return max(protocol_index)+1,sn_list
        return None

    def upload_banco_de_dados_repair(self,nome_arquivo,diretorio_entrada):
        path = os.path.join(diretorio_entrada,'passed')
        dataset = pd.read_csv(os.path.join(path,nome_arquivo))
        data_to_upload = dataset[dataset['test_ok'] == 0]
        data_to_upload = data_to_upload.drop_duplicates(subset='sn', keep='first')
        data_to_remove = dataset[dataset['test_ok'] == 1]
        data_to_remove = data_to_remove.drop_duplicates(subset='sn', keep='first')
        data_to_remove = data_to_remove[['sn','ts','test_routine','test_ok']]

        if data_to_upload.shape[0] == 0 and data_to_remove.shape[0] == 0:
            return True
        protocol_index,sn_bd_list = self.get_protocol_index_and_sn_list()
        data_to_upload = data_to_upload[['sn','ts','test_routine','test_ok']]
        try:
            start_time = time.perf_counter()
            print("Subindo no Bando de Dados Repair" + nome_arquivo)
            connection = mysql.connector.connect(**self.db_config_repair)
            cursor = connection.cursor()
        except Exception as e:
            print("Falha ao Subir no Bando de Dados Repair" + nome_arquivo)
            error_message = str(e)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open('error_log_fh.txt', 'a') as log_file:
                log_file.write("Error at " + now + '\n')
                log_file.write(error_message + ' ' + nome_arquivo + '\n')
            return False
        try:
            for index, row in data_to_upload.iterrows():
                if row['sn'] in sn_bd_list or row['sn'] in data_to_remove['sn'].to_list():
                    continue
                row = row.fillna('')
                protocol = 'JIG1.' + str(protocol_index).zfill(6)
                query = f"INSERT IGNORE INTO {tabela_repair} (protocol,sn,ts,user_id,repair_state_id,repair_type_id) VALUES (%s, %s, %s, %s, %s,%s)"
                current_timestamp = datetime.now(timezone.utc)
                record = (protocol,row['sn'],current_timestamp,"10971","1","1")
                cursor.execute(query, record)
                protocol_index += 1
            for index, row in data_to_remove.iterrows():
                if row['sn'] in sn_bd_list:
                    row = row.fillna('')
                    query = f"DELETE FROM {tabela_repair} WHERE sn = %s and repair_state_id = 1"
                    record = [row['sn']]
                    cursor.execute(query, record)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            cursor.close()
            connection.close()
            print("Falha ao Subir no Bando de Dados Repair" + nome_arquivo)
            error_message = str(e)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open('error_log_fh.txt', 'a') as log_file:
                log_file.write("Error at " + now + '\n')
                log_file.write(error_message + ' ' + nome_arquivo + '\n')
            return False
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Upload de {nome_arquivo}, elapsed time : {elapsed_time}" )
        return True

    def upload_banco_de_dados(self,nome_arquivo,diretorio_entrada,passed_or_failed):
        path = os.path.join(diretorio_entrada,passed_or_failed)
        dataset = pd.read_csv(os.path.join(path,nome_arquivo))
        try:
            start_time = time.perf_counter()
            print("Subindo no Bando de Dados " + nome_arquivo)
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
        except Exception as e:
            print("Falha ao Subir no Bando de Dados " + nome_arquivo)
            error_message = str(e)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open('error_log_fh.txt', 'a') as log_file:
                log_file.write("Error at " + now + '\n')
                log_file.write(error_message + ' ' + nome_arquivo + '\n')
            return False
        if passed_or_failed == 'passed':
            data_to_upload = dataset[['sn','ts','test_routine','test_ok','output_voltage']]
            try:
                for index, row in data_to_upload.iterrows():
                    row = row.fillna('')
                    query = "INSERT IGNORE INTO equip (sn,ts,test_routine,test_ok,output_voltage) VALUES (%s, %s, %s, %s,%s)"
                    record = (row['sn'],self.date_s3_to_db(row['ts']),row['test_routine'],row['test_ok'],row['output_voltage'])
                    cursor.execute(query, record)
                connection.commit()
                cursor.close()
                connection.close()
            except Exception as e:
                cursor.close()
                connection.close()
                print("Falha ao Subir no Bando de Dados " + nome_arquivo)
                error_message = str(e)
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                with open('error_log_fh.txt', 'a') as log_file:
                    log_file.write("Error at " + now + '\n')
                    log_file.write(error_message + ' ' + nome_arquivo + '\n')
                return False
        else:
            data_to_upload = dataset[['ts','sn','test_routine','len','len_mis','output_voltage','test_ok']]
            try:
                for index, row in data_to_upload.iterrows():
                    row = row.fillna('')
                    query = "INSERT IGNORE INTO equip (sn,ts,test_routine,len,len_mis,output_voltage,test_ok) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    record = (row['sn'],self.date_s3_to_db(row['ts']),row['test_routine'],row['len'],row['len_mis'],row['output_voltage'],row['test_ok'])
                    cursor.execute(query, record)
            except Exception as e:
                cursor.close()
                connection.close()
                print("Falha ao Subir no Bando de Dados " + nome_arquivo)
                error_message = str(e)
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                with open('error_log_fh.txt', 'a') as log_file:
                    log_file.write("Error at " + now + '\n')
                    log_file.write(error_message + ' ' + nome_arquivo + '\n')
                return False
            
            connection.commit()
            cursor.close()
            connection.close()
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        print(f"Upload de {nome_arquivo}, elapsed time : {elapsed_time}" )
        return True
  
    def upload_data_lake(self,nome_arquivo,diretorio_entrada):#,diretorio_saida = None):
        #Definindo as chaves de acesso e o bucket
        s3=boto3.client('s3',aws_access_key_id=self.access_key,aws_secret_access_key=self.secret_access_key)
    
        #Upload de todos os csv no diretorio 
        nome_arquivo_temp = nome_arquivo
        nome_arquivo = "-".join(nome_arquivo.split("-")[:-1])+'.csv'
        file_path_temp = os.path.join(diretorio_entrada,nome_arquivo_temp)
        file_path = os.path.join(diretorio_entrada,nome_arquivo)
        data = pd.read_csv(file_path_temp);
        data = data.drop("test_routine",axis = 1)
        data = data.drop("output_voltage",axis = 1)
        data.to_csv(file_path,index=None)
        splitted=nome_arquivo.split(".csv")
        splitted=splitted[0].split("-")
        y=splitted[3]
        m=splitted[4]
        d=splitted[5]
        codigo_identificacao = splitted[2]
        table = 'testing-data-jnhr120' if splitted[1] == 'jnhr120' else 'testing-data-jnhr220'
        key=f"efd/{table}/code={codigo_identificacao}/year=%04d/month=%02d/day=%02d/%s" % (int(y), int(m), int(d), nome_arquivo)
        try:
            start_time = time.perf_counter()
            print("Subindo no Data Lake " + nome_arquivo) 
            s3.upload_file(file_path,self.bucket,key)     
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            print(f"Upload de {nome_arquivo}, elapsed time : {elapsed_time}" )
        except Exception as e:
            print("Falha ao Subir no Data Lake " + nome_arquivo)
            error_message = str(e)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open('error_log_fh.txt', 'a') as log_file:
                log_file.write("Error at " + now + '\n')
                log_file.write(error_message + ' ' + nome_arquivo + '\n')
                os.remove(file_path)
            return False
        os.remove(file_path)
        return True
    
    def date_s3_to_db(self,date): 
        date_ts=datetime.strptime(date,"%Y-%m-%dT%H:%M:%S.%fZ")
        new_date_ts=date_ts.strftime('%Y-%m-%d %H:%M:%S')
        return new_date_ts
