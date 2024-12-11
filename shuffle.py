import random
import concurrent.futures
import time
import math
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def shuffle_lines(lines):
    random.shuffle(lines)
    return lines

def process_chunk(chunk):
    logging.info(f"Началась обработка чанка из {len(chunk)} строк.")
    shuffled_chunk = shuffle_lines(chunk)
    logging.info(f"Чанк из {len(chunk)} строк обработан.")
    return shuffled_chunk

def shuffle_large_file(filepath, output_filepath, num_threads=None):
    start_time = time.time()

    if num_threads is None:
        num_threads = concurrent.futures.ThreadPoolExecutor()._max_workers

    logging.info(f"Используется потоков: {num_threads}")

    with open(filepath, 'r') as f:
        total_lines = sum(1 for _ in f)

    logging.info(f"Всего строк в файле: {total_lines}")

    chunk_size = math.ceil(total_lines / num_threads)

    logging.info(f"Размер части: {chunk_size} строк")

    with open(filepath, 'r') as f:
        chunks = []
        chunk = []
        lines_read = 0
        for line in f:
            chunk.append(line)
            lines_read += 1
            if len(chunk) == chunk_size:
                chunks.append(chunk)
                logging.info(f"Чанк из {len(chunk)} строк добавлен. Всего строк обработано: {lines_read}.")
                chunk = []
        if chunk:
            chunks.append(chunk)
            logging.info(f"Последний чанк из {len(chunk)} строк добавлен. Всего строк обработано: {lines_read}.\n")

    logging.info(f"Файл разделен на {len(chunks)} частей.")
    
    clear_console()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(process_chunk, chunks)

    with open(output_filepath, 'w') as outfile:
        for idx, chunk_result in enumerate(results, start=1):
            outfile.writelines(chunk_result)
            logging.info(f"Чанк {idx}/{len(chunks)} записан в выходной файл.")

    end_time = time.time()
    clear_console()
    logging.info(f"Строки в файле перемешаны и записаны в {output_filepath}.")
    logging.info(f"Время выполнения: {end_time - start_time:.2f} секунд")

# настройки
input_file = 'test.txt'  # файл, который надо перемешать
output_file = 'output_test.txt' # название перемешанного файла
num_threads = None # кол-во потоков

shuffle_large_file(input_file, output_file, num_threads)
