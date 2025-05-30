import csv
import os
import mysql.connector
from dotenv import load_dotenv

def conectar_banco():
    print("[LOG] Iniciando conexão com o banco...")
    load_dotenv()

    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_CORRIER_PRODUCAO"),
            user=os.getenv("DB_CORRIER_USER"),
            password=os.getenv("DB_CORRIER_PASS"),
            database="corrier"
        )
        print("[LOG] Conexão com o banco estabelecida.")
        return conn
    except Exception as e:
        print(f"[ERRO] Erro ao conectar com o banco de dados: {e}")
        return None

def obter_ultimo_status(cursor, encoid):
    print(f"[LOG] Consultando último status para encoid={encoid}...")
    query = """
        SELECT status, `timestamp`
        FROM encomendas_status
        WHERE encoid = %s
        ORDER BY `data` DESC, hora DESC
        LIMIT 1
    """
    cursor.execute(query, (encoid,))
    resultado = cursor.fetchone()
    print(f"[LOG] Resultado obtido: {resultado}")
    return resultado

def executar(arquivo_csv, nome_arquivo_saida=None):
    print(f"[LOG] Executando atualização a partir do arquivo: {arquivo_csv}")
    updates = []

    conexao = conectar_banco()
    if not conexao:
        print("[ERRO] Conexão com banco falhou, abortando execução.")
        return

    try:
        cursor = conexao.cursor()

        with open(arquivo_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            linha_num = 0
            for linha in reader:
                linha_num += 1
                if not linha:
                    print(f"[LOG] Linha {linha_num} vazia, pulando.")
                    continue
                encoid = linha[0].strip()
                print(f"[LOG] Processando linha {linha_num}: encoid={encoid}")
                if not encoid.isdigit():
                    print(f"[AVISO] ID inválido ignorado na linha {linha_num}: {encoid}")
                    continue
                try:
                    resultado = obter_ultimo_status(cursor, encoid)
                    if resultado:
                        status, timestamp = resultado
                        update_sql = (
                            f"UPDATE corrier.encomendas SET ultimostatid = {status}, ultimostatdata = '{timestamp}' "
                            f"WHERE encoid = {encoid};"
                        )
                        updates.append(update_sql)
                        print(f"[LOG] SQL gerado para encoid {encoid}: {update_sql}")
                    else:
                        print(f"[AVISO] Status não encontrado para encoid {encoid} (linha {linha_num})")
                except Exception as e:
                    print(f"[ERRO] Erro ao consultar encoid {encoid} na linha {linha_num}: {e}")

        cursor.close()
        conexao.close()
        print("[LOG] Cursor e conexão fechados.")

    except mysql.connector.Error as err:
        print(f"[ERRO] Erro ao trabalhar com o banco: {err}")
        return

    if not updates:
        print("[LOG] Nenhum UPDATE gerado.")
        return

    if nome_arquivo_saida is None:
        base = os.path.basename(arquivo_csv)          
        nome_arquivo_saida = os.path.splitext(base)[0] + ".sql"  

    if "input" in arquivo_csv:
        caminho_saida = arquivo_csv.replace("input", "output")
    else:
        caminho_saida = os.path.join("temp", "output", nome_arquivo_saida)

    caminho_saida = os.path.splitext(caminho_saida)[0] + ".sql"

    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)

    try:
        with open(caminho_saida, "w", encoding="utf-8") as f:
            f.write("\n".join(updates))
        print(f"[LOG] Arquivo de saída '{caminho_saida}' criado com sucesso.")
    except Exception as e:
        print(f"[ERRO] Erro ao escrever o arquivo de saída: {e}")
