import pandas as pd
import multiprocessing as mp
import threading
import time


def process_data(df):

    media_geral = df['QT_VOTOS_NOMINAIS_VALIDOS'].mean()


    media_partido = df.groupby('SG_PARTIDO')['QT_VOTOS_NOMINAIS_VALIDOS'].mean()
    melhor_partido = media_partido.idxmax()
    valor_melhor_partido = media_partido.max()


    media_cargo = df.groupby('DS_CARGO')['QT_VOTOS_NOMINAIS_VALIDOS'].mean()
    melhor_cargo = media_cargo.idxmax()
    valor_melhor_cargo = media_cargo.max()

    return {
        "media_geral": media_geral,
        "melhor_partido": (melhor_partido, valor_melhor_partido),
        "melhor_cargo": (melhor_cargo, valor_melhor_cargo)
    }


def multiprocessing(df):

    num_cores = mp.cpu_count()
    chunks = []
    for i in range(num_cores):

        chunk = df.iloc[i::num_cores]
        chunks.append(chunk)

    with mp.Pool(processes=num_cores) as pool:
        results = pool.map(process_data, chunks)


    total_votos = sum([chunk['QT_VOTOS_NOMINAIS_VALIDOS'].sum() for chunk in chunks])
    total_registros = len(df)
    media_geral = total_votos / total_registros


    full_res = process_data(df)
    return full_res

def multithreading(df):

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


    return process_data(df)


def main():
    file_path = 'dados/votacao.csv'

    print("Carregando dados...")

    try:
        df = pd.read_csv(file_path, sep=';', encoding='latin-1')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')


    df = df[df['NM_MUNICIPIO'] == 'CAMPINAS']

    print("-" * 30)


    start_time = time.time()
    res_mp = multiprocessing(df)
    end_time = time.time()
    time_mp = end_time - start_time
    print(f"Tempo Multiprocessing: {time_mp:.4f} segundos")


    start_time = time.time()
    res_mt = multithreading(df)
    end_time = time.time()
    time_mt = end_time - start_time
    print(f"Tempo Multithreading: {time_mt:.4f} segundos")

    print("-" * 30)


    print(f"Média Geral de Votos (Campinas): {res_mp['media_geral']:.2f}")

    partido, v_partido = res_mp['melhor_partido']
    print(f"Partido com maior média: {partido} ({v_partido:.2f} votos)")

    cargo, v_cargo = res_mp['melhor_cargo']
    print(f"Cargo com maior média: {cargo} ({v_cargo:.2f} votos)")

    print("-" * 30)


    if time_mp < time_mt:
        print(f"Resultado: Multiprocessing foi mais rápido por {time_mt - time_mp:.4f}s")
    else:
        print(f"Resultado: Multithreading foi mais rápido por {time_mp - time_mt:.4f}s")


if __name__ == '__main__':
    main()
