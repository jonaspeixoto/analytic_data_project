import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

# Carregar variáveis de ambiente
load_dotenv()
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')


def mapear_df(df):
    mapeamento_uf = {
        'Paraná': 'PR',
        'Paraíba': 'PB',
        'Tocantins': 'TO',
        'São Paulo': 'SP',
        'Piauí': 'PI',
        'Roraima': 'RR',
        'Amapá': 'AP',
        'Rio Grande do Norte': 'RN',
        'Mato Grosso': 'MT',
        'Amazonas': 'AM',
        'Rondônia': 'RO',
        'Goiás': 'GO',
        'Mato Grosso do Sul': 'MS',
        'Espírito Santo': 'ES',
        'Santa Catarina': 'SC',
        'Acre': 'AC',
        'Alagoas': 'AL',
        'Bahia': 'BA',
        'Ceará': 'CE',
        'Rio Grande do Sul': 'RS',
        'Distrito Federal': 'DF',
        'Sergipe': 'SE',
        'Pará': 'PA',
        'Pernambuco': 'PE',
        'Minas Gerais': 'MG',
        'Rio de Janeiro': 'RJ',
        'Maranhão': 'MA'
    }

    df['UF'] = df['UF'].map(mapeamento_uf)

    df['Isento'] = df['Isento'].fillna(False).astype(bool)

    df['CEP'] = df['CEP'].fillna('')
    df['Endereço'] = df['Endereço'].fillna('')

    return df

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

        # Tratando dados de CPF/CNPJ
        df = df.replace({np.nan: None, pd.NaT: None})
        df['CPF/CNPJ'] = df['CPF/CNPJ'].astype(str)
        df['CPF/CNPJ'] = df['CPF/CNPJ'].str.replace(r'\D', '', regex=True)

        duplicados_clientes = []

        # Queries
        insert_query_clientes = """
        INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) 
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (cpf_cnpj) DO NOTHING RETURNING id;
        """

        insert_query_planos = """
        INSERT INTO tbl_planos (descricao, valor) 
        VALUES (%s, %s) ON CONFLICT (descricao) DO NOTHING RETURNING id;
        """

        insert_query_status = """
        INSERT INTO tbl_status_contrato (status) 
        VALUES (%s) ON CONFLICT (status) DO NOTHING RETURNING id;
        """

        insert_query_contratos = """
        INSERT INTO tbl_cliente_contratos (cliente_id, plano_id, dia_vencimento, isento, endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for indice, row in df.iterrows():
            # Inserir dados do cliente
            cursor.execute(insert_query_clientes, (
                row['Nome/Razão Social'],
                row['Nome Fantasia'],
                row['CPF/CNPJ'],
                row['Data Nasc.'],
                row['Data Cadastro cliente']
            ))

            if cursor.rowcount > 0:  
                cliente_id = cursor.fetchone()[0]  
            else:  
                cursor.execute("SELECT id FROM tbl_clientes WHERE cpf_cnpj = %s;", (row['CPF/CNPJ'],))
                cliente_id_result = cursor.fetchone()
                cliente_id = cliente_id_result[0] if cliente_id_result else None

            if cliente_id is None:
                duplicados_clientes.append(row['CPF/CNPJ'])

            # Inserir dados do plano
            cursor.execute(insert_query_planos, (
                row['Plano'],
                row['Plano Valor']
            ))

            cursor.execute("SELECT id FROM tbl_planos WHERE descricao = %s;", (row['Plano'],))
            plano_id = cursor.fetchone()
            print(plano_id)


            cursor.execute(insert_query_status, (
                row['Status'],
            ))

            cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = %s;", (row['Status'],))
            status_id = cursor.fetchone()
            print(status_id)

            
            # Inserir dados na tabela de contratos
            cursor.execute(insert_query_contratos, (
                cliente_id,
                plano_id,
                row['Vencimento'],
                row['Isento'],
                row['Endereço'],
                row['Número'],
                row['Bairro'],
                row['Cidade'],
                row['Complemento'],
                row['CEP'],
                row['UF'],
                status_id,
            ))

        # Commit das transações
        conn.commit()
        cursor.close()
        conn.close()

        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao conectar: {e}")


# Carregar dados do Excel e chamar a função

df = pd.read_excel('dados_importacao.xlsx')
df_mapeado = mapear_df(df)
print(df_mapeado)

inserir_dados(df_mapeado)
