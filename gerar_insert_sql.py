import csv
import sys
import os

def gerar_insert_sql_csv(arquivo_csv, arquivo_sql='inserir_encomendas.sql'):
    if not os.path.exists(arquivo_csv):
        print(f"Erro: Arquivo '{arquivo_csv}' não encontrado.")
        return

    cabecalho = """INSERT INTO corrier.encomendas_status (
  encoid, status, statusdesc, `data`, hora, sedex, `timestamp`, cafid, funcid, agid, wap, recebedor, encostd_id
)
VALUES
"""

    valores = []
    with open(arquivo_csv, newline='', encoding='utf-8') as csvfile:
        leitor = csv.reader(csvfile, delimiter=';')
        for linha in leitor:
            if len(linha) != 3:
                continue 
            encoid, data, hora = linha
            linha_sql = f"  ({encoid}, 1, 'ENTREGA REALIZADA', '{data}', '{hora}', NULL, '2025-05-09 08:23:32', 0, 36851, 0, 0, 0, 0)"
            valores.append(linha_sql)

    if not valores:
        print("Nenhuma linha válida encontrada no CSV.")
        return

    query_final = cabecalho + ",\n".join(valores) + ";"

    with open(arquivo_sql, 'w', encoding='utf-8') as f:
        f.write(query_final)

    print(f"Arquivo '{arquivo_sql}' gerado com sucesso!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python gerar_insert_sql.py <entrada.csv> [saida.sql]")
        sys.exit(1)

    entrada = sys.argv[1]
    saida = sys.argv[2] if len(sys.argv) > 2 else 'inserir_encomendas.sql'

    gerar_insert_sql_csv(entrada, saida)
