import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np


load_dotenv()
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')


def inserir_dados(df):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        print('Conexão realizada com sucesso')

        # Trantando dados cpf/cnpj
        df = df.replace({np.nan: None, pd.NaT: None})
        df['CPF/CNPJ'] = df['CPF/CNPJ'].astype(str)
        df['CPF/CNPJ'] = df['CPF/CNPJ'].str.replace(r'\D', '', regex=True)

        duplicados_clientes = []


        # Querys
        insert_query_clientes = f"""
        INSERT INTO {'tbl_clientes'} (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) 
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (cpf_cnpj) DO NOTHING RETURNING id;
        """

        insert_query_planos = f"""
        INSERT INTO {'tbl_planos'} (descricao, valor) 
        VALUES (%s, %s) ON CONFLICT (descricao) DO NOTHING RETURNING id;
        """

        insert_query_status = f"""
        INSERT INTO {'tbl_status_contrato'} (status) 
        VALUES (%s) ON CONFLICT (status) DO NOTHING RETURNING id;
        """



        for indice, row in df.iterrows():
            cursor.execute(insert_query_clientes, (
                row['Nome/Razão Social'],
                row['Nome Fantasia'],
                row['CPF/CNPJ'],
                row['Data Nasc.'],
                row['Data Cadastro cliente']
            ))

            # Verifica se o cpf já existe na base se sim insere na lista de cliente duplicado
            if cursor.rowcount == 0:  
                duplicados_clientes.append(row['CPF/CNPJ'])


            # Inserir dados do plano
            cursor.execute(insert_query_planos, (
            row['Plano'],
            row['Plano Valor']
            ))

            # Inserir dados do status
            cursor.execute(insert_query_status, (
            row['Status'],
            ))
            



        conn.commit()
        cursor.close()
        conn.close()

        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao conectar : {e}")


df = pd.read_excel('dados_importacao.xlsx')

inserir_dados(df)


