import csv

def executar(arquivo_csv, arquivo_saida):
    updates = []

    with open(arquivo_csv, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for linha in reader:
            if not linha:
                continue
            encoid = linha[0].strip()
            updates.append(f"UPDATE corrier.encomendas SET embid=0 WHERE encoid={encoid};")

    with open(arquivo_saida, "w", encoding="utf-8") as f:
        f.write("\n".join(updates))
