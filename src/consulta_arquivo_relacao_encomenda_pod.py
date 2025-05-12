import csv
import os
import mysql.connector
from dotenv import load_dotenv

def remover_caracteres(valor):
    return valor.replace('.', '').replace('-', '').replace('/', '').strip()

def executar(input_csv_path, output_csv_path, log_path="consulta_erros.log", delimitador=";"):
    print("Iniciando execução...")
    load_dotenv()

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_CORRIER_PRODUCAO"),
            user=os.getenv("DB_CORRIER_USER"),
            password=os.getenv("DB_CORRIER_PASS"),
            database="corrier"
        )
        print("Conexão com o banco estabelecida.")
    except Exception as e:
        print("Erro ao conectar com o banco de dados:", e)
        return

    cursor = conn.cursor(dictionary=True)

    try:
        with open(input_csv_path, newline='', encoding='utf-8') as csvfile_in, \
             open(output_csv_path, "w", newline='', encoding='utf-8') as csvfile_out, \
             open(log_path, "a", encoding='utf-8') as log_file:

            reader = csv.DictReader(csvfile_in, delimiter=delimitador)
            writer = csv.writer(csvfile_out, delimiter=delimitador)

            sucesso = erro = 0
            total = 0

            for row in reader:
                total += 1
                awb = remover_caracteres(row.get("AWB", ""))
                pedido = remover_caracteres(row.get("Pedido", ""))
                nota = remover_caracteres(row.get("Nota Fiscal", ""))

                if not awb or not pedido or not nota:
                    print(f"[{total}] Linha com dados incompletos: {row}")
                    log_file.write(f"Layout inválido: {row}\n")
                    erro += 1
                    continue

                print(f"[{total}] Consultando: AWB={awb}, Pedido={pedido}, Nota={nota}")

                query = """
                    SELECT e.awb, img.encoimg_id, img.encoimg_data
                    FROM encomendas e
                    INNER JOIN encomendas_img img ON e.encoid = img.encoid
                    WHERE e.awb = %s AND e.pedido = %s AND e.nfiscal = %s AND img.encoimg_tipo = 0
                """

                cursor.execute(query, (awb, pedido, nota))
                resultados = cursor.fetchall()

                if resultados:
                    resultado = resultados[0]
                    print(f"[{total}] Encontrado: {resultado}")
                    writer.writerow([resultado['awb'], resultado['encoimg_id'], resultado['encoimg_data']])

                else:
                    print(f"[{total}] Sem imagem para: {awb}")
                    log_file.write(f"Erro: Sem imagem para AWB: {awb} - Pedido: {pedido} - Nota Fiscal: {nota}\n")
                    writer.writerow([awb])
                    erro += 1

    except Exception as e:
        print("Erro ao processar o arquivo CSV:", e)
    finally:
        cursor.close()
        conn.close()
        print(f"Conexão com o banco encerrada.")
        print(f"Finalizado: {total} registros lidos | {sucesso} sucesso(s) | {erro} erro(s)")
