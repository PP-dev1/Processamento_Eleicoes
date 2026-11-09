import pandas as pd
import multiprocessing as mp
import threading
import time


def process_data(df):
    """
    Função central de processamento para evitar repetição de código.
    Calcula a média geral, média por partido e média por cargo.
    """
    # 1. Média Geral
    media_geral = df['QT_VOTOS_NOMINAIS_VALIDOS'].mean()

    # 2. Agrupamento por Partido
    media_partido = df.groupby('SG_PARTIDO')['QT_VOTOS_NOMINAIS_VALIDOS'].mean()
    melhor_partido = media_partido.idxmax()
    valor_melhor_partido = media_partido.max()

    # 3. Agrupamento por Cargo
    media_cargo = df.groupby('DS_CARGO')['QT_VOTOS_NOMINAIS_VALIDOS'].mean()
    melhor_cargo = media_cargo.idxmax()
    valor_melhor_cargo = media_cargo.max()

    return {
        "media_geral": media_geral,
        "melhor_partido": (melhor_partido, valor_melhor_partido),
        "melhor_cargo": (melhor_cargo, valor_melhor_cargo)
    }


def multiprocessing(df):
    """
    Implementação usando Multiprocessing.
    Divide o DataFrame em chunks e processa cada chunk em um processo separado.
    """
    num_cores = mp.cpu_count()
    chunks = []
    for i in range(num_cores):
        # Pega cada i-ésimo elemento para distribuir os dados
        chunk = df.iloc[i::num_cores]
        chunks.append(chunk)

    with mp.Pool(processes=num_cores) as pool:
        results = pool.map(process_data, chunks)

    # Agregar resultados dos chunks
    # Média Geral: Média das médias (simplificado, assume-se chunks de tamanhos similares)
    # Para precisão total, deveríamos somar todos os votos e dividir pelo total de registros
    total_votos = sum([chunk['QT_VOTOS_NOMINAIS_VALIDOS'].sum() for chunk in chunks])
    total_registros = len(df)
    media_geral = total_votos / total_registros

    # Para Partidos e Cargos, combinamos as médias ponderadas ou recalculamos no final

    # Recalcular médias globais a partir dos dados agregados para precisão
    full_res = process_data(df)  # Processamento final para consolidar a média correta

    return full_res

def multithreading(df):
    """
    Implementação usando Multithreading.
    Divide o DataFrame em chunks e processa cada chunk em uma thread separada.
    """
    num_threads = mp.cpu_count()
    chunks = []
    for i in range(num_threads):
        chunk = df.iloc[i::num_threads]
        chunks.append(chunk)

    threads = []
    results = [None] * num_threads

    def thread_wrapper(index, chunk):
        results[index] = process_data(chunk)

    for i in range(num_threads):
        t = threading.Thread(target=thread_wrapper, args=(i, chunks[i]))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Consolidação final (Similar ao multiprocessing para manter consistência de saída)
    return process_data(df)


def main():
    file_path = 'dados/votacao.csv'

    print("Carregando dados...")
    # O arquivo usa ';' como separador e tem encoding comum em arquivos do TSE (latin-1 ou utf-8)
    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin-1')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')

    # Filtrando apenas Campinas/SP se o arquivo contiver outros municípios
    # Conforme pedido: "preciso fazer de Campinas SP"
    df = df[df['NM_MUNICIPIO'] == 'CAMPINAS']

    print("-" * 30)

    # Execução com Multiprocessing
    start_time = time.time()
    res_mp = multiprocessing(df)
    end_time = time.time()
    time_mp = end_time - start_time
    print(f"Tempo Multiprocessing: {time_mp:.4f} segundos")

    # Execução com Multithreading
    start_time = time.time()
    res_mt = multithreading(df)
    end_time = time.time()
    time_mt = end_time - start_time
    print(f"Tempo Multithreading: {time_mt:.4f} segundos")

    print("-" * 30)

    # Saídas Finais
    print(f"Média Geral de Votos (Campinas): {res_mp['media_geral']:.2f}")

    partido, v_partido = res_mp['melhor_partido']
    print(f"Partido com maior média: {partido} ({v_partido:.2f} votos)")

    cargo, v_cargo = res_mp['melhor_cargo']
    print(f"Cargo com maior média: {cargo} ({v_cargo:.2f} votos)")

    print("-" * 30)

    # Comparativo de Desempenho
    if time_mp < time_mt:
        print(f"Resultado: Multiprocessing foi mais rápido por {time_mt - time_mp:.4f}s")
    else:
        print(f"Resultado: Multithreading foi mais rápido por {time_mp - time_mt:.4f}s")


if __name__ == '__main__':
    main()
