# version 1.1.2


import os
import sys
import pandas as pd
from loguru import logger
import datetime
import glob

# Добавление файла с логами. По достижении 2 MB будет архивироваться и создаваться новый
logger.add("ls_and_msisdn_to_uki.log", format="{time} {level} {message}", level="DEBUG", rotation="2 MB", compression="zip")

# Пути к папкам с файлами
ls_and_msisdn_dir = 'D:\VC_UNISIM\input_GLONASS_MSISDN\\'
uki_and_msisdn_dir = 'D:\VC_UNISIM\svyazka_UKI_MSISDN\\'
result_msisdn_v_uki = 'D:\VC_UNISIM\\result_converter_MSISDN_v_UKI\\'


@logger.catch()
def convert_ls_and_msisdn_to_uki():
    # Проверим формат файлов и объединим все файлы с УКИ и МСИСДН в один dataframe,
    # так как нам нужно будет пробежаться по всем файлам
    uki_and_msisdn_files = os.listdir(path=uki_and_msisdn_dir)
    for file in uki_and_msisdn_files:
        if file[len(file) - 4:] != ".csv":
            logger.error(f"In dir {uki_and_msisdn_dir} not just csv-files!")
            sys.exit(4)

    all_files_uki_and_msisdn = glob.glob(os.path.join(uki_and_msisdn_dir, "*.csv"))
    df_uki_and_msisdn = (pd.read_csv(f, header=None, names=["UKI", "MSISDN"], sep=';') for f in all_files_uki_and_msisdn)
    concatenated_df_uki_and_msisdn = pd.concat(df_uki_and_msisdn, ignore_index=True)
    logger.debug("Created: concatenated_df_uki_and_msisdn")

    # Точно также сделаем и с файлом с ЛС и МСИСДН. Но возьмем последний файл
    time_max = datetime.datetime(2000, 1, 1, 1, 1)
    latest_file = ''
    ls_and_msisdn_files = os.listdir(path=ls_and_msisdn_dir)
    for file in ls_and_msisdn_files:
        time_file = file[file.rfind("-") + 1:file.rfind('.')]
        time_file = time_file[:4] + '-' + time_file[4:6] + '-' + time_file[6:8] + ' ' + time_file[8:10] + ':' + time_file[10:12]
        format_time = "%Y-%m-%d %H:%M"
        time_file = datetime.datetime.strptime(time_file, format_time)
        if time_file > time_max:
            time_max = time_file
            latest_file = file
    logger.debug(f"Found latest file: {latest_file} in dir: {ls_and_msisdn_dir}\nwith time created: {time_max}")

    if latest_file[len(latest_file) - 4:] != ".txt":
        logger.error(f"File: {latest_file} in dir: {ls_and_msisdn_dir}")
        sys.exit(4)

    # Добавление предупреждающего уведомления о конвертации файлов
    employee_answers = input(f"Вы хотите конвертировать файл {latest_file}\n"
                             f"Из директории:\n"
                             f"{ls_and_msisdn_dir}? y/n\n")

    if employee_answers == 'n' or employee_answers == 'not':
        logger.error("The employee stopped the program")
        sys.exit(4)

    df_ls_and_msisdn = pd.read_csv(ls_and_msisdn_dir + latest_file, header=None, names=["LS"])
    df_ls_and_msisdn["LS"] = df_ls_and_msisdn["LS"].map(lambda x: x.replace(';', ''))
    df_ls_and_msisdn["MSISDN"] = df_ls_and_msisdn["LS"].map(lambda x: x[12:])
    df_ls_and_msisdn["LS"] = df_ls_and_msisdn["LS"].map(lambda x: x.replace(x[12:], ''))
    logger.debug("Created df_ls_and_msisdn")

    # Перед тем как делать объединение УКИ и МСИСДН с ЛС и МСИСДН проверим ошибки
    # 1) В файле ЛС и МСИСДН должно быть 2 поля
    # 2) В файле УКИ и МСИСДН в столбце УКИ длина каждой строки должна быть равна 11-ти символам
    # 3) В столбце МСИСДН все строки должны начинаться с 7-ки
    # 4) В файле УКИ и МСИСДН в столбце УКИ должны быть только цифры
    if df_ls_and_msisdn.shape[1] != 2:
        logger.error(f"Columns != 2 in file {latest_file}")
        sys.exit(5)


    for uki_errors in concatenated_df_uki_and_msisdn["UKI"].apply(lambda x: len(str(x)) == 11 and str(x).isdigit()):
        if not uki_errors:
            logger.error("In UKI and MSISDN files lenght != 11 or not just numbers in string")
            sys.exit(5)

    for ls_msisdn_errors in df_ls_and_msisdn["MSISDN"].apply(lambda x: str(x).startswith('7')):
        if not ls_msisdn_errors:
            logger.error("In file LS and MSISDN string don't start with 7")
            sys.exit(5)

    for uki_msisdn_errors in concatenated_df_uki_and_msisdn["MSISDN"].apply(lambda x: str(x).startswith('7')):
        if not uki_msisdn_errors:
            logger.error("In file LS and MSISDN string don't start with 7")
            sys.exit(5)

    # Объединим 2 dataframe
    concatenated_df_uki_and_msisdn[["UKI", "MSISDN"]] = concatenated_df_uki_and_msisdn[["UKI", "MSISDN"]].astype(str)
    df_ls_and_msisdn[["LS", "MSISDN"]] = df_ls_and_msisdn[["LS", "MSISDN"]].astype(str)
    df_merged_msisdn = pd.merge(df_ls_and_msisdn, concatenated_df_uki_and_msisdn).drop(columns=["LS", "MSISDN"])
    logger.debug("Created finaly dataframe")

    # Теперь создадим название файла для УКИ
    merged_msisdn_file_name = f"VC-output-UKI-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".csv"
    logger.debug(f"Created name for dataframe: {merged_msisdn_file_name}")

    # Выгрузим dataframe df_merged_msisdn в файл merged_msisdn_file_name
    df_merged_msisdn.to_csv(result_msisdn_v_uki + merged_msisdn_file_name, index=None, header=False)
    logger.debug("Upload the file")

    # Сообщение об окончании программы
    employee_last_answers = input(f"Результаты конвертации размещены в директории:\n"
                                  f"{result_msisdn_v_uki}, имя файла:\n"
                                  f"{merged_msisdn_file_name}\n"
                                  f"Понятно? y/n\n")


convert_ls_and_msisdn_to_uki()
