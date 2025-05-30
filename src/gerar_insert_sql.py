import csv

def executar(arquivo_csv, arquivo_saida):
    values = []

    try:
        with open(arquivo_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for linha in reader:
                if len(linha) < 5:
                    print(f"Linha ignorada por não ter 5 campos: {linha}")
                    continue
                try:
                    encoid, status, descricao, data, hora = linha
                    values.append(
                        f"({encoid}, {status}, '{descricao}', '{data}', '{hora}', NULL, '{data} {hora}', 0, 36851, 0, 0, 0, 0)"
                    )
                except ValueError as e:
                    print(f"Erro ao processar linha {linha}: {e}")

    except FileNotFoundError:
        print(f"Erro: Arquivo '{arquivo_csv}' não encontrado.")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    if not values:
        print("Nenhum dado válido encontrado no CSV.")
        return

    insert_stmt = (
        "INSERT INTO corrier.encomendas_status "
        "(encoid, status, statusdesc, `data`, hora, sedex, `timestamp`, cafid, funcid, agid, wap, recebedor, encostd_id)\n"
        "VALUES\n" + ",\n".join(values) + ";"
    )

    try:
        with open(arquivo_saida, "w", encoding="utf-8") as f:
            f.write(insert_stmt)
        print(f"Arquivo de saída '{arquivo_saida}' criado com sucesso.")
    except Exception as e:
        print(f"Erro ao escrever o arquivo de saída: {e}")
