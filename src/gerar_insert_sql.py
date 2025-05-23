import csv

def executar(arquivo_csv, arquivo_saida):
    values = []

    with open(arquivo_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for linha in reader:
            if len(linha) < 3:
                continue
            encoid, status, descricao, data, hora = linha
            values.append(
                f"({encoid}, {status}, '{descricao}', '{data}', '{hora}', NULL, '{data} {hora}', 0, 36851, 0, 0, 0, 0)"
            )

    if not values:
        return

    insert_stmt = (
        "INSERT INTO corrier.encomendas_status "
        "(encoid, status, statusdesc, `data`, hora, sedex, `timestamp`, cafid, funcid, agid, wap, recebedor, encostd_id)\n"
        "VALUES\n" + ",\n".join(values) + ";"
    )

    with open(arquivo_saida, "w", encoding="utf-8") as f:
        f.write(insert_stmt)
