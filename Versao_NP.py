import pandas as pd
import os
import time

#Substitua pelo seu caminho da pasta Dados
caminho_pasta = "C:\\Users\\caiob\\OneDrive\\Desktop\\UCB\\Concorrente e Paralela\\Projeto Final\\Dados"

# Geração arquivo Consolidado.cvs
def gerar_consolidado(caminho_pasta):
    tl = time.time()
    lista_dataframes = []
    for nome_arquivo in os.listdir(caminho_pasta):
        caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
        df = pd.read_csv(caminho_completo)
        lista_dataframes.append(df)
    tlf = time.time()
    print(f"Tempo lendo arquivos: {tlf - tl}")
    
    tc = time.time()
    df_resultado = pd.concat(lista_dataframes, ignore_index=True)
    tcf = time.time()
    print(f"Tempo concatenando: {tcf - tc}")
    return df_resultado


resultado = gerar_consolidado(caminho_pasta)
t0 = time.time()
diretorio_script = os.path.dirname(os.path.abspath(__file__))
nome_arquivo_consolidado = os.path.join(diretorio_script, 'Consolidado.csv')
resultado.to_csv(nome_arquivo_consolidado, index=False)
tf = time.time()
print(f"Tempo criando csv: {tf - t0}")
