import pandas as pd
import os
import time

diretorio_script = os.path.dirname(os.path.abspath(__file__))
diretorio_script += '\\Dados'

def gerar_consolidado():

    tl = time.time()
    lista_csv = [f for f in os.listdir(diretorio_script) if f.endswith('.csv')]
    tlf = time.time()
    BaseDados = pd.DataFrame()

    for arquivo in lista_csv:
        tc = time.time()
        df =  pd.read_csv(os.path.join(diretorio_script, arquivo))
        BaseDados = pd.concat([BaseDados, df], ignore_index=True)
        tcf = time.time()

    t0 = time.time()
    BaseDados.to_csv('Consolidado.csv', index=False, encoding='utf-8')
    tf = time.time()

    print(f"Tempo criar csv: {tf - t0}")
    print(f"Tempo concatenando: {tcf - tc}")
    print(f"Tempo lendo: {tlf - tl}")

gerar_consolidado()

