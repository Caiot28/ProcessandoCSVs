import pandas as pd
import os
import time
import concurrent.futures
import threading

# Função genérica para calcular as metas
def calcular_metas(df, col_julgados, col_distm_casos_novos, col_dessobrestados, col_suspensos, multiplicador):
    
    # Verifica se as colunas necessárias existem no DataFrame
    colunas_necessarias = [col_julgados, col_distm_casos_novos, col_suspensos]
    if col_dessobrestados:
        colunas_necessarias.append(col_dessobrestados)

    for col in colunas_necessarias:
        if col not in df.columns:
            print(f"Erro: Coluna '{col}' não encontrada no DataFrame para esta meta.")
            return None

    # Meta 1
    if col_dessobrestados: 
        denominador = (df[col_distm_casos_novos].sum() +
                       df[col_dessobrestados].sum() -
                       df[col_suspensos].sum())
    # Demais metas
    else: 
        denominador = (df[col_distm_casos_novos].sum() -
                       df[col_suspensos].sum())

    if denominador != 0:
        #Calculo principal
        return (df[col_julgados].sum() * multiplicador) / denominador
    else:
        print(f"Denominador = 0, divisão falhou")
        return None

def metas_justica_estadual(df_estadual):
    """Calcula todas as metas para a Justiça Estadual."""
    if df_estadual.empty: return {}
    return {
        'Meta 1': calcular_metas(df_estadual, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_estadual, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 8.0)),
        'Meta 2B': calcular_metas(df_estadual, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.0)),
        'Meta 2C': calcular_metas(df_estadual, 'julgm2_c', 'distm2_c', None, 'suspm2_c', (1000 / 9.5)),
        'Meta 2ANT': calcular_metas(df_estadual, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_estadual, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 6.5)),
        'Meta 4B': calcular_metas(df_estadual, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_estadual, 'julgm6', 'distm6', None, 'suspm6', 100.0),
        'Meta 7A': calcular_metas(df_estadual, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 5.0)),
        'Meta 7B': calcular_metas(df_estadual, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 5.0)),
        'Meta 8A': calcular_metas(df_estadual, 'julgm8_a', 'distm8_a', None, 'suspm8_a', (1000 / 7.5)),
        'Meta 8B': calcular_metas(df_estadual, 'julgm8_b', 'distm8_b', None, 'suspm8_b', (1000 / 9.0)),
        'Meta 10A': calcular_metas(df_estadual, 'julgm10_a', 'distm10_a', None, 'suspm10_a', (1000 / 9.0)),
        'Meta 10B': calcular_metas(df_estadual, 'julgm10_b', 'distm10_b', None, 'suspm10_b', (1000 / 10.0))
    }

def metas_justica_trabalho(df_trabalho):
    """Calcula todas as metas para a Justiça do Trabalho."""
    if df_trabalho.empty: return {}
    return {
        'Meta 1': calcular_metas(df_trabalho, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_trabalho, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.4)), 
        'Meta 2ANT': calcular_metas(df_trabalho, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0)
    }

def metas_justica_federal(df_federal):
    """Calcula todas as metas para a Justiça Federal."""
    if df_federal.empty: return {}
    return {
        'Meta 1': calcular_metas(df_federal, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_federal, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 8.5)), 
        'Meta 2B': calcular_metas(df_federal, 'julgm2_b', 'distm2_b', None, 'suspm2_b', 100.0),
        'Meta 2ANT': calcular_metas(df_federal, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_federal, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 7.0)),
        'Meta 4B': calcular_metas(df_federal, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_federal, 'julgm6', 'distm6', None, 'suspm6', (1000 / 3.5)),
        'Meta 7A': calcular_metas(df_federal, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 3.5)),
        'Meta 7B': calcular_metas(df_federal, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 3.5)),
        'Meta 8A': calcular_metas(df_federal, 'julgm8_a', 'distm8_a', None, 'suspm8_a', (1000 / 7.5)),
        'Meta 8B': calcular_metas(df_federal, 'julgm8_b', 'distm8_b', None, 'suspm8_b', (1000 / 9.0)),
        'Meta 10': calcular_metas(df_federal, 'julgm10_a', 'distm10_a', None, 'suspm10_a', 100.0)
    }

def metas_justica_militar_uniao(df_jmu):
    """Calcula todas as metas para a Justiça Militar da União."""
    if df_jmu.empty: return {}
    return {
        'Meta 1': calcular_metas(df_jmu, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_jmu, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.5)), 
        'Meta 2B': calcular_metas(df_jmu, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_jmu, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_jmu, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.5)),
        'Meta 4B': calcular_metas(df_jmu, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 9.9))
    }

def metas_justica_militar_estadual(df_jme):
    """Calcula todas as metas para a Justiça Militar Estadual."""
    if df_jme.empty: return {}
    return {
        'Meta 1': calcular_metas(df_jme, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_jme, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.0)), 
        'Meta 2B': calcular_metas(df_jme, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.5)),
        'Meta 2ANT': calcular_metas(df_jme, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_jme, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.5)),
        'Meta 4B': calcular_metas(df_jme, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 9.9))
    }

def metas_justica_eleitoral(df_tse):
    """Calcula todas as metas para Justiça Eleitoral (TRE's)."""
    if df_tse.empty: return {}
    return {
        'Meta 1': calcular_metas(df_tse, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_tse, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 7.0)), 
        'Meta 2B': calcular_metas(df_tse, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_tse, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_tse, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.0)),
        'Meta 4B': calcular_metas(df_tse, 'julgm4_b', 'distm4_b', None, 'suspm4_b', (1000 / 5.0))
    }

def metas_tst(df_tst):
    """Calcula todas as metas para o Tribunal Superior do Trabalho (TST)."""
    if df_tst.empty: return {}
    return {
        'Meta 1': calcular_metas(df_tst, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2A': calcular_metas(df_tst, 'julgm2_a', 'distm2_a', None, 'suspm2_a', (1000 / 9.5)), 
        'Meta 2B': calcular_metas(df_tst, 'julgm2_b', 'distm2_b', None, 'suspm2_b', (1000 / 9.9)),
        'Meta 2ANT': calcular_metas(df_tst, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0)
    }

def metas_stj(df_stj):
    """Calcula todas as metas para o Superior Tribunal de Justiça (STJ)."""
    if df_stj.empty: return {}
    return {
        'Meta 1': calcular_metas(df_stj, 'julgados_2025', 'casos_novos_2025', 'dessobrestados_2025', 'suspensos_2025', 100.0),
        'Meta 2ANT': calcular_metas(df_stj, 'julgm2_ant', 'distm2_ant', None, 'suspm2_ant', 100.0),
        'Meta 4A': calcular_metas(df_stj, 'julgm4_a', 'distm4_a', None, 'suspm4_a', (1000 / 9.0)),
        'Meta 4B': calcular_metas(df_stj, 'julgm4_b', 'distm4_b', None, 'suspm4_b', 100.0),
        'Meta 6': calcular_metas(df_stj, 'julgm6_a', 'distm6_a', None, 'suspm6_a', (1000 / 7.5)),
        'Meta 7A': calcular_metas(df_stj, 'julgm7_a', 'distm7_a', None, 'suspm7_a', (1000 / 7.5)),
        'Meta 7B': calcular_metas(df_stj, 'julgm7_b', 'distm7_b', None, 'suspm7_b', (1000 / 7.5)),
        'Meta 8': calcular_metas(df_stj, 'julgm8', 'distm8', None, 'suspm8', (1000 / 10.0)),
        'Meta 10': calcular_metas(df_stj, 'julgm10', 'distm10', None, 'suspm10', (1000 / 10.0))
    }

diretorio_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Dados')

#Define a função que será ativa de acordo com o ramo
funcoes_por_ramo = {
    'Justiça Estadual': metas_justica_estadual,
    'Justiça do Trabalho': metas_justica_trabalho,
    'Justiça Federal': metas_justica_federal,
    'Justiça Militar da União': metas_justica_militar_uniao,
    'Justiça Militar Estadual': metas_justica_militar_estadual,
    'Justiça Eleitoral': metas_justica_eleitoral
}

meta_ids = ['1', '2A', '2B', '2C', '2ANT', '4A', '4B', '6', '7A', '7B', '8A', '8B', '8', '10A', '10B', '10']

def ler_arquivos(caminho_arquivo):
    thread_atual = threading.current_thread()
    print(f"Começou na thread '{thread_atual.name}' (ID: {thread_atual.ident})")

    df = pd.read_csv(caminho_arquivo)
    linhas = []
    for ramo_justica_atual in df['ramo_justica'].unique():

        # Lógica para Tribunais Superiores
        if ramo_justica_atual == 'Tribunais Superiores':
                
            df_superiores = df[df['ramo_justica'] == 'Tribunais Superiores'].copy()

            # Itera sobre as siglas de tribunal para identificar STJ/TST
            for sigla_tribunal_atual in df_superiores['sigla_tribunal'].unique():

                df_filtrado_tribunal = df_superiores[df_superiores['sigla_tribunal'] == sigla_tribunal_atual].copy()

                if sigla_tribunal_atual == 'STJ':
                    funcao_a_chamar = metas_stj
                    ramo_para_registro = 'Superior Tribunal de Justiça'
                elif sigla_tribunal_atual == 'TST':
                    funcao_a_chamar = metas_tst
                    ramo_para_registro = 'Tribunal Superior do Trabalho' 
                else:
                    # Se for 'Tribunais Superiores' mas não é STJ nem TST, trata como desconhecido
                    print(f"Metas inexistentes para {sigla_tribunal_atual} em {os.path.basename(caminho_arquivo)}")
                    continue
    
                #Se não estiver vazio
                if not df_filtrado_tribunal.empty:
                    print(f"\nCalculando metas para o arquivo: {os.path.basename(caminho_arquivo)}")
                    resultados_calculados = funcao_a_chamar(df_filtrado_tribunal)
                        
                    info_tribunal = {
                        'sigla_tribunal': sigla_tribunal_atual,
                        'ramo_justica': ramo_para_registro
                    }
                    #Linha que vai para ResumoMetas.csv
                    linha_completa = {**info_tribunal, **resultados_calculados}

                        # Preenche com 'NA' as metas não calculadas/aplicáveis
                    for meta_id in meta_ids:
                        if f'Meta {meta_id}' not in linha_completa:
                            linha_completa[f'Meta {meta_id}'] = 'NA'
                    linhas.append(linha_completa)
                else:
                    print(f"Nenhum dado filtrado para {ramo_para_registro} ({sigla_tribunal_atual}) no arquivo '{os.path.basename(caminho_arquivo)}'.")

            # Lógica para os demais tribunais
        elif ramo_justica_atual in funcoes_por_ramo:
            # Filtra o DataFrame pelo ramo de justiça atual
            df_ramo_filtrado = df[df['ramo_justica'] == ramo_justica_atual].copy()
        
            if not df_ramo_filtrado.empty:
                print(f"\nCalculando metas para o arquivo: {os.path.basename(caminho_arquivo)}")
                resultados_calculados = funcoes_por_ramo[ramo_justica_atual](df_ramo_filtrado)
                    
                info_tribunal = {
                    'sigla_tribunal': df_ramo_filtrado['sigla_tribunal'].iloc[0] if 'sigla_tribunal' in df_ramo_filtrado.columns and not df_ramo_filtrado['sigla_tribunal'].empty else None,
                    'ramo_justica': ramo_justica_atual
                }

                linha_completa = {**info_tribunal, **resultados_calculados}

                    # Preenche com 'NA' as metas que não foram calculadas/aplicáveis para este ramo
                for meta_id in meta_ids:
                    if f'Meta {meta_id}' not in linha_completa:
                        linha_completa[f'Meta {meta_id}'] = 'NA'

                linhas.append(linha_completa)
            else:
                print(f"Nenhum dado filtrado para o ramo '{ramo_justica_atual}' no arquivo '{os.path.basename(caminho_arquivo)}'.")
    return df, linhas        

# Geração arquivo Consolidado.cvs
def gerar_consolidado(df_consolidado):
    t0 = time.time()
    df_consolidado.to_csv('ConsolidadoParalelizado.csv', index=False)
    tf = time.time()
    print(f"Tempo criando Consolidado.csv: {tf - t0}")

def gerar_resumo_metas(df_resumoM):
    t0 = time.time()
    df_resumoM.to_csv('ResumoMetasParalelizado.csv', index=False)
    tf = time.time()
    print(f"Tempo criando ResumoMetas.csv: {tf - t0}")

def gerar_consolidado_paralelizado():
    lista_arquivos = []
    linhas_metas = []
    total_linhas = []
    lista_df = []
    for nome_arquivo in os.listdir(diretorio_script):
        if nome_arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio_script, nome_arquivo)
            lista_arquivos.append(caminho_completo)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        linhas_metas = list(executor.map(ler_arquivos, lista_arquivos))
        for df_lido, linhas in linhas_metas:
            lista_df.append(df_lido)
            total_linhas.extend(linhas)

    #Cria o dataframe de ResumoMetas
    df_consolidado = pd.concat(lista_df, ignore_index=True)
    resumo_metas = pd.DataFrame(total_linhas)
    colunas = ['sigla_tribunal', 'ramo_justica']
    colunas_metas_ordenadas = [f'Meta {m}' for m in meta_ids]
    colunas += colunas_metas_ordenadas

    # Reorganiza e preenche colunas ausentes no DataFrame final
    resumo_metas = resumo_metas.reindex(columns=colunas).fillna('NA')
    return resumo_metas, df_consolidado

if __name__ == "__main__":
    t0 = time.time()
    resumo_metas, consolidado = gerar_consolidado_paralelizado()
    gerar_consolidado(consolidado)
    gerar_resumo_metas(resumo_metas)
    t1 = time.time()
    print(f"Tempo total: {t1-t0}")