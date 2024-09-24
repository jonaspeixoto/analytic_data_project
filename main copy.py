import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Credenciais do banco de dados
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

# Função para inserir dados no banco
def inserir_dados(df, table_name_clientes, table_name_planos, table_name_status, table_name_contratos):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        print('Conexão realizada com sucesso!')

        # Tratar valores ausentes como None
        df = df.replace({np.nan: None, pd.NaT: None})

        # Garantir que a coluna CPF/CNPJ seja string
        df['CPF/CNPJ'] = df['CPF/CNPJ'].astype(str)

        # Listas para armazenar duplicados
        duplicados_clientes = []
        duplicados_planos = []

        # Queries de inserção
        insert_query_clientes = f"""
        INSERT INTO {table_name_clientes} (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) 
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (cpf_cnpj) DO NOTHING RETURNING id;
        """

        insert_query_planos = f"""
        INSERT INTO {table_name_planos} (descricao, valor) 
        VALUES (%s, %s) ON CONFLICT (descricao) DO NOTHING RETURNING id;
        """

        insert_query_status = f"""
        INSERT INTO {table_name_status} (status) 
        VALUES (%s) ON CONFLICT (status) DO NOTHING RETURNING id;
        """

        insert_query_contratos = f"""
        INSERT INTO {table_name_contratos} (cliente_id, plano_id, status_id, dia_vencimento, isento, endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_complemento, cep, uf)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Inserir os dados no banco
        for _, row in df.iterrows():
            # Inserir dados do cliente
            cursor.execute(insert_query_clientes, (
                row['Nome/Razão Social'],
                row['Nome Fantasia'],
                row['CPF/CNPJ'],
                row['Data Nasc.'],
                row['Data Cadastro cliente']
            ))
            if cursor.rowcount == 0:  # Cliente já existe
                duplicados_clientes.append(row['CPF/CNPJ'])

        #     # Inserir dados do plano
        #     cursor.execute(insert_query_planos, (
        #         row['Plano'],
        #         row['Plano Valor']
        #     ))
        #     if cursor.rowcount == 0:  # Plano já existe
        #         duplicados_planos.append(row['Plano'])

        #     # Inserir dados do status
        #     cursor.execute(insert_query_status, (
        #         row['Status'],
        #     ))

        #     # Recuperar IDs inseridos
        #     cliente_id = cursor.fetchone()[0]
        #     plano_id = cursor.fetchone()[0]
        #     status_id = cursor.fetchone()[0]

        #     # Inserir dados na tabela de contratos
        #     cursor.execute(insert_query_contratos, (
        #         cliente_id,
        #         plano_id,
        #         status_id,
        #         row['Vencimento'],
        #         row['Isento'],
        #         row['Endereço'],
        #         row['Número'],
        #         row['Bairro'],
        #         row['Cidade'],
        #         row['Complemento'],
        #         row['CEP'],
        #         row['UF']
        #     ))

        # # Confirmar as mudanças no banco de dados
        conn.commit()
        cursor.close()
        conn.close()

        # print("Dados inseridos com sucesso!")
        # if duplicados_clientes:
        #     print(f"CPF/CNPJ duplicados encontrados: {duplicados_clientes}")
        # if duplicados_planos:
        #     print(f"Planos duplicados encontrados: {duplicados_planos}")

    except Exception as e:
        print(f"Erro ao conectar ou inserir dados: {e}")

# Ler o arquivo Excel (substitua pelo caminho correto do seu arquivo)
df = pd.read_excel('dados_importacao.xlsx')

# Selecionar as colunas relacionadas ao plano e ao cliente
df_planos = df[['Plano', 'Plano Valor']]
df_clientes = df[['Nome/Razão Social', 'Nome Fantasia', 'CPF/CNPJ', 'Data Nasc.', 'Data Cadastro cliente']]
df_status = df[['Status']]
df_contratos = df[['Vencimento', 'Isento', 'Endereço', 'Número', 'Bairro', 'Cidade', 'Complemento', 'CEP', 'UF', 'CPF/CNPJ', 'Plano', 'Status']]

# Inserir os dados nas tabelas
inserir_dados(df_clientes, 'tbl_clientes', 'tbl_planos', 'tbl_status_contrato', 'tbl_cliente_contratos')
