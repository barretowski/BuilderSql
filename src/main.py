import sys
import importlib
import os

def main():
    if len(sys.argv) < 3:
        print("Uso: python main.py <classe> <arquivo_input>")
        sys.exit(1)

    nome_classe = sys.argv[1]
    nome_arquivo = sys.argv[2]

    modulo_path = f"src.{nome_classe}"
    try:
        modulo = importlib.import_module(modulo_path)
    except ModuleNotFoundError:
        print(f"Erro: módulo '{modulo_path}' não encontrado.")
        sys.exit(1)

    caminho_input = os.path.join("temp", "input", nome_arquivo)
    nome_arquivo_saida = os.path.splitext(nome_arquivo)[0] + ".sql"
    caminho_output = os.path.join("temp", "output", nome_arquivo_saida)

    if hasattr(modulo, "executar"):
        modulo.executar(caminho_input, caminho_output)
    else:
        print(f"O módulo '{nome_classe}' precisa definir uma função 'executar(entrada, saida)'.")

if __name__ == "__main__":
    main()
