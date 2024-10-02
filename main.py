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

duplicados_clientes = []
registros_nao_importados = []

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
        print('dbname')
        cursor = conn.cursor()
        print('Conexão realizada com sucesso')
        print("inserindo dados")

        df = df.replace({np.nan: None, pd.NaT: None})
        df['CPF/CNPJ'] = df['CPF/CNPJ'].astype(str)
        df['CPF/CNPJ'] = df['CPF/CNPJ'].str.replace(r'\D', '', regex=True)


        # Querys e
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
        INSERT INTO tbl_cliente_contratos (cliente_id, plano_id, dia_vencimento, isento, endereco_logradouro, endereco_numero, endereco_bairro, endereco_cidade, endereco_complemento, endereco_cep, endereco_uf, status_id,desconto, mac, ip)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s);
        """

        insert_query_cliente_contatos = """
        INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_Id, contato)
        VALUES ('%s', '%s', '%s');
        """

        total_registros_importados = 0

        for indice, row in df.iterrows():
            try:
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
                    duplicados_clientes.append(row['CPF/CNPJ'])

                cursor.execute(insert_query_planos, (
                    row['Plano'],
                    row['Plano Valor']
                ))

                cursor.execute("SELECT id FROM tbl_planos WHERE descricao = %s;", (row['Plano'],))
                plano_id = cursor.fetchone()[0]

                cursor.execute(insert_query_status, (
                    row['Status'],
                ))

                cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = %s;", (row['Status'],))
                status_id = cursor.fetchone()[0]

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
                    row['Desconto'],
                    row['MAC'],
                    row['IP'],
                    
                ))

                if row['Celulares']: 
                    cursor.execute(insert_query_cliente_contatos %(
                        cliente_id,
                        1,
                        row['Celulares'],
                    ))
                if row['Telefones']: 
                   cursor.execute(insert_query_cliente_contatos %(
                        cliente_id,
                        2,
                        row['Telefones'],
                    ))

                if row['Emails']: 
                  cursor.execute(insert_query_cliente_contatos %(
                        cliente_id,
                        3,
                        row['Emails'],
                    ))

                total_registros_importados += 1

            except Exception as e:
                registros_nao_importados.append({
                    'linha': indice + 1,
                    'motivo': str(e)
                })

        conn.commit()
        cursor.close()
        conn.close()

        print("Dados inseridos com sucesso!")
        print(f"Total de registros importados: {total_registros_importados}")
        print("Registros não importados:")
        for registro in registros_nao_importados:
            print(f"Linha {registro['linha']}: {registro['motivo']}")

    except Exception as e:
        print(f"Erro ao conectar: {e}")

df = pd.read_excel('dados.xlsx')
df_mapeado = mapear_df(df)

inserir_dados(df_mapeado)

print("Clientes duplicados:", duplicados_clientes)
