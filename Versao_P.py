import pandas as pd
import os
import time
import concurrent.futures

#Substitua pelo seu caminho da pasta Dados
caminho_pasta = "C:\\Users\\caiob\\OneDrive\\Desktop\\UCB\\Concorrente e Paralela\\Projeto Final\\Dados"

def ler_arquivos(caminho_arquivo):
    df = pd.read_csv(caminho_arquivo)
    
    return df
      
def gerar_consolidado_paralelizado():
    lista_arquivos = []
    for nome_arquivo in os.listdir(caminho_pasta):
        caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
        lista_arquivos.append(caminho_completo)

    tl = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        lista_dataframes = list(executor.map(ler_arquivos, lista_arquivos))
    tlf = time.time()
    t0 = time.time()
    df_concatenado = pd.concat(lista_dataframes, ignore_index=True)
    tf = time.time()
    t1 = time.time()
    diretorio_script = os.path.dirname(os.path.abspath(__file__))
    nome_arquivo_consolidado = os.path.join(diretorio_script, 'Consolidado_p.csv')
    df_concatenado.to_csv(nome_arquivo_consolidado, index=False)
    t2 = time.time()

    print(f"Tempo concatenando: {tf - t0}")
    print(f"Tempo criando csv: {t2 - t1}")
    print(f"Tempo lendo arquivos: {tlf - tl}")