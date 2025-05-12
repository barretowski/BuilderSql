import csv
import os
import mysql.connector
from dotenv import load_dotenv

def remover_caracteres(valor):
    return valor.replace('.', '').replace('-', '').replace('/', '').strip()

def executar(input_csv_path, output_csv_path, log_path="consulta_erros.log", delimitador=";"):
    load_dotenv()

    conn = mysql.connector.connect(
        host=os.getenv("DB_CORRIER_PRODUCAO"),
        user=os.getenv("DB_CORRIER_USER"),
        password=os.getenv("DB_CORRIER_PASS"),
        database="corrier"
    )

    cursor = conn.cursor(dictionary=True)

    with open(input_csv_path, newline='', encoding='utf-8') as csvfile_in, \
         open(output_csv_path, "w", newline='', encoding='utf-8') as csvfile_out, \
         open(log_path, "a", encoding='utf-8') as log_file:

        reader = csv.DictReader(csvfile_in, delimiter=delimitador)
        writer = csv.writer(csvfile_out, delimiter=delimitador)

        sucesso = erro = 0

        for row in reader:
            awb = remover_caracteres(row.get("AWB", ""))
            pedido = remover_caracteres(row.get("Pedido", ""))
            nota = remover_caracteres(row.get("Nota Fiscal", ""))

            if not awb or not pedido or not nota:
                log_file.write(f"Layout inválido: {row}\n")
                erro += 1
                continue

            query = """
                SELECT e.awb, img.encoimg_id, img.encoimg_data
                FROM encomendas e
                INNER JOIN encomendas_img img ON e.encoid = img.encoid
                WHERE e.awb = %s AND e.pedido = %s AND e.nfiscal = %s AND img.encoimg_tipo = 0
            """

            cursor.execute(query, (awb, pedido, nota))
            resultado = cursor.fetchone()

            if resultado:
                writer.writerow([resultado['awb'], resultado['encoimg_id'], resultado['encoimg_data']])
                sucesso += 1
            else:
                log_file.write(f"Erro: Sem imagem para AWB: {awb} - Pedido: {pedido} - Nota Fiscal: {nota}\n")
                writer.writerow([awb])
                erro += 1

    cursor.close()
    conn.close()
    print(f"Concluído: {sucesso} sucesso(s), {erro} erro(s)")
