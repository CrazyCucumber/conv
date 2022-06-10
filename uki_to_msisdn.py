# version 1.1.2
# Программа конвертации UKI ---> MSISDN


# Импортируем нужные библиотеки
import os
import sys
import time
import pandas as pd
from loguru import logger
import datetime
import glob

# Импорт класса, который завершит программы,
# если user не даст ответ на input()
from program_completion_module import Inp

# Добавление файла с логами. По достижении 2 MB будет архивироваться и создаваться новый
logger.add("uki_to_msisdn.log", format="{time} {level} {message}", level="DEBUG", rotation="2 MB", compression="zip")

# Пути к входящим папкам с файлами
uki_dir = 'X:\VC_UNISIM\input_VC_UKI\\'
ls_and_msisdn_dir = 'X:\VC_UNISIM\input_GLONASS_MSISDN\\'
uki_and_msisdn_dir = 'X:\VC_UNISIM\svyazka_UKI_MSISDN\\'
# Пути к исходящим папкам с файлами
result_uki_v_msisdn = 'X:\VC_UNISIM\\result_converter_UKI_v_MSISDN\\'


@logger.catch()
def convert_uki_to_msisdn():
    # Проверим формат файлов и объединим все файлы с УКИ и МСИСДН в один dataframe,
    # так как нам нужно будет работать со всеми файлами
    uki_and_msisdn_files = os.listdir(path=uki_and_msisdn_dir)
    for file in uki_and_msisdn_files:
        if file[len(file) - 4:] != ".csv":
            logger.error(f"In dir {uki_and_msisdn_dir} not just csv-files!")
            sys.exit(2)

    all_files_uki_and_msisdn = glob.glob(os.path.join(uki_and_msisdn_dir, "*.csv"))
    df_uki_and_msisdn = (pd.read_csv(f, header=None, names=["UKI", "MSISDN"], sep=';') for f in
                         all_files_uki_and_msisdn)
    concatenated_df_uki_and_msisdn = pd.concat(df_uki_and_msisdn, ignore_index=True)
    logger.debug("Created: concatenated_df_uki_and_msisdn")

    # Теперь найдем последнюю директорию с файлами UKI
    all_uki_dir = os.listdir(path=uki_dir)
    latest_uki_dir = all_uki_dir[len(all_uki_dir) - 1] + '\\'
    logger.debug(f"We are find latest dir with UKI-files\nIt's {latest_uki_dir}")

    # Проверка формата файлов с УКИ
    uki_files = os.listdir(path=uki_dir + latest_uki_dir)
    for file in uki_files:
        if file[len(file) - 4:] != ".txt" and file[len(file) - 4:] != ".csv":
            logger.error(f"In dir {uki_dir + latest_uki_dir} not just txt-files or csv-files!")
            sys.exit(2)

    # Добавление предупреждающего уведомления о конвертации файлов
    print(f"Программа конвертирует последние сохраненные файлы:\n"
          f"{', '.join(uki_files)}\n"
          f"Из папки:\n"
          f"{uki_dir + latest_uki_dir}\n"
          f"Продолжить? y/n")

    employee_answers = Inp().get()

    if employee_answers is None:
        logger.debug("The employee did not enter anything, the program was closed")
        print("Программа закрыватеся по не ответу пользователя через несколько секунд")
        time.sleep(2)
        sys.exit(9)
    elif employee_answers == 'n' or employee_answers == 'not' or employee_answers == 'N':
        if latest_uki_dir != '0000\\':
            new_dir = '000' + str(int(latest_uki_dir[latest_uki_dir.rfind('0') + 1:len(latest_uki_dir) - 1]) + 1)
            new_dir = new_dir[len(new_dir) - 5:]
        else:
            new_dir = '0001'
        print(f"Создайте новую папку {new_dir} в директории {uki_dir},\n"
              f"скопируйте в нее файлы, которые хотите конвертировать и запустите программу снова.\n"
              f"Текущий сеанс программы будет автоматически завершен через 30 секунд\n")

        logger.debug("The employee stopped the program")
        time.sleep(30)
        sys.exit(0)
    elif employee_answers == 'y' or employee_answers == 'yes' or employee_answers == 'Y':
        # Теперь создадим директорию для файлов, которые будем выгружать
        try:
            flag = True
            result_uki_v_msisdn_dir = result_uki_v_msisdn + latest_uki_dir
            os.mkdir(result_uki_v_msisdn_dir)
            logger.debug(f"Created {result_uki_v_msisdn + latest_uki_dir} dir"
                         f"Finaly UKI_v_MSISDN dir is {result_uki_v_msisdn + latest_uki_dir}")
        except FileExistsError:
            flag = False
            logger.debug(f"Dir {result_uki_v_msisdn_dir} already created")
            all_created_uki_v_msisdn_dir = os.listdir(path=result_uki_v_msisdn)
            created_uki_array = []
            for c in all_created_uki_v_msisdn_dir:
                if latest_uki_dir[:len(latest_uki_dir) - 1] in c:
                    created_uki_array.append(c)
            latest_created_uki_dir = max(created_uki_array)
            if '-' in latest_created_uki_dir:
                latest_created_uki_dir = latest_created_uki_dir[:latest_created_uki_dir.rfind('-')] + '-' + \
                                         str(int(latest_created_uki_dir[latest_created_uki_dir.find('-') + 1:]) + 1)
            else:
                latest_created_uki_dir = latest_created_uki_dir + '-1'
            result_uki_v_msisdn_dir = result_uki_v_msisdn + latest_created_uki_dir
            os.mkdir(result_uki_v_msisdn_dir)
            logger.debug(f"Created new dir: {latest_created_uki_dir}")

        # Теперь будем проходиться по каждому файлу с УКИ,
        # и будем делать merge с dataframe concatenated_df_uki_and_msisdn
        # Но перед тем как это выполнить - нужно
        # 1) Выбрать только 1 поле в файле с УКИ
        # 2) Проверить, что в файле с УКИ в каждом после количество цифр равно 11
        # 3) Проверить, что каждый первый символ в файле с УКИ равен 1
        # 4) Проверить, что в каждой строке только цифры
        result_uki_v_msisdn_files = []
        for uki_file in uki_files:
            df_uki = pd.read_csv(uki_dir + latest_uki_dir + uki_file, header=None, names=["UKI"])
            df_uki = pd.DataFrame(df_uki.iloc[:, 0])
            logger.debug("We choise first column in df_uki")
            for uki_errors in df_uki["UKI"].apply(
                    lambda x: len(str(x)) == 11 and str(x).startswith('1') and str(x).isdigit()):
                if not uki_errors:
                    logger.error("In UKI-file lenght != 11 or string don't start with 1 or not just numbers in string")
                    sys.exit(3)

            # Объединение УКИ с УКИ и МСИСДН
            df_merged_uki = pd.merge(df_uki, concatenated_df_uki_and_msisdn).astype(str)
            logger.debug("merged UKI with UKI and MSISDN")

            df_merged_uki.loc[:, "UKI"] = "210006929285"
            logger.debug("Change UKI to LS")

            # Подготовка финального dataframe, который выгрузим в txt-файл
            df_merged_uki["UKI"] = df_merged_uki["UKI"].map(lambda x: x.replace(x[:], ';' + x[:] + ';'))
            df_merged_uki["MSISDN"] = df_merged_uki["MSISDN"].map(lambda x: x.replace(x[:], x[:] + ';;;'))
            df_merged_uki["all_data"] = df_merged_uki["UKI"].str.cat(df_merged_uki["MSISDN"], sep="")
            df_merged_uki = df_merged_uki.drop(columns=["UKI", "MSISDN"])
            logger.debug("Created final dataframe")

            # Теперь создадим название файла для ЛС и МСИСДН
            number_of_file = uki_file[uki_file.rfind('-'):uki_file.rfind('.')]
            merged_msisdn_file_name = f"VC-output-MSISDN-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + \
                                      number_of_file + ".txt"
            result_uki_v_msisdn_files.append(merged_msisdn_file_name)
            logger.debug(f"Created name for dataframe: {merged_msisdn_file_name}")

            # Теперь выгрузим файл
            df_merged_uki.to_csv(result_uki_v_msisdn_dir + '\\' + merged_msisdn_file_name, index=None, header=False)
            logger.debug("Uploaded the file")

        # Сообщение об окончании программы
        if flag:
            print(f"Результаты конвертации размещены в директории:\n"
                  f"{result_uki_v_msisdn_dir}\n"
                  f"Список файлов:"
                  f"{', '.join(result_uki_v_msisdn_files)}\n"
                  f"Запомните или запишите на листочке папку, где сохранены файлы после конвертации.\n "
                  f"Программа успешно провела операцию конвертации\n"
                  f"и будет автоматически завершена через 30 секунд.")

            logger.debug("Program close")
            time.sleep(30)
            sys.exit(0)
        else:
            print(f"Конвертация уже была ранее проведена для входных файлов из папки:\n"
                  f"{uki_dir + latest_uki_dir}\n"
                  f"поэтому результаты этого запроса на конвертацию будут размещены в директории:\n"
                  f"{result_uki_v_msisdn_dir}\n"
                  f"Список файлов:"
                  f"{', '.join(result_uki_v_msisdn_files)}\n"
                  f"Запомните или запишите на листочке папку, где сохранены файлы после конвертации.\n "
                  f"Программа успешно провела операцию конвертации\n"
                  f"и будет автоматически завершена через 30 секунд.\n")

            logger.debug("Program close")
            time.sleep(30)
            sys.exit(0)
    else:
        print(f"Вы ввели неверный символ. Проверьте\n"
              f"переключились ли Вы на английский язык и\n"
              f"повторите попытку ввода символов y или n. При\n"
              f"неправильном вводе еще раз программа будет\n"
              f"автоматически завершена и Вам придется\n"
              f"запустить ее снова, если операция конвертации\n"
              f"не проведена.\n")
        employee_answers = Inp().get()

        if employee_answers == 'n' or employee_answers == 'not' or employee_answers == 'N':
            if latest_uki_dir != '0000\\':
                new_dir = '000' + str(
                    int(latest_uki_dir[latest_uki_dir.rfind('0') + 1:len(latest_uki_dir) - 1]) + 1)
                new_dir = new_dir[len(new_dir) - 5:]
            else:
                new_dir = '0001'
            print(f"Создайте новую папку {new_dir} в директории {uki_dir},\n"
                  f"скопируйте в нее файлы, которые хотите конвертировать и запустите программу снова.\n"
                  f"Текущий сеанс программы будет автоматически завершен через 30 секунд\n")

            logger.debug("The employee stopped the program")
            time.sleep(30)
            sys.exit(0)
        elif employee_answers == 'y' or employee_answers == 'yes' or employee_answers == 'Y':
            # Теперь создадим директорию для файлов, которые будем выгружать
            try:
                flag = True
                result_uki_v_msisdn_dir = result_uki_v_msisdn + latest_uki_dir
                os.mkdir(result_uki_v_msisdn_dir)
                logger.debug(f"Created {result_uki_v_msisdn + latest_uki_dir} dir"
                             f"Finaly UKI_v_MSISDN dir is {result_uki_v_msisdn + latest_uki_dir}")
            except FileExistsError:
                flag = False
                logger.debug(f"Dir {result_uki_v_msisdn_dir} already created")
                all_created_uki_v_msisdn_dir = os.listdir(path=result_uki_v_msisdn)
                created_uki_array = []
                for c in all_created_uki_v_msisdn_dir:
                    if latest_uki_dir[:len(latest_uki_dir) - 1] in c:
                        created_uki_array.append(c)
                latest_created_uki_dir = max(created_uki_array)
                if '-' in latest_created_uki_dir:
                    latest_created_uki_dir = latest_created_uki_dir[:latest_created_uki_dir.rfind('-')] + '-' + \
                                             str(int(latest_created_uki_dir[latest_created_uki_dir.find('-') + 1:]) + 1)
                else:
                    latest_created_uki_dir = latest_created_uki_dir + '-1'
                result_uki_v_msisdn_dir = result_uki_v_msisdn + latest_created_uki_dir
                os.mkdir(result_uki_v_msisdn_dir)
                logger.debug(f"Created new dir: {latest_created_uki_dir}")

            # Теперь будем проходиться по каждому файлу с УКИ,
            # и будем делать merge с dataframe concatenated_df_uki_and_msisdn
            # Но перед тем как это выполнить - нужно
            # 1) Выбрать только 1 поле в файле с УКИ
            # 2) Проверить, что в файле с УКИ в каждом после количество цифр равно 11
            # 3) Проверить, что каждый первый символ в файле с УКИ равен 1
            # 4) Проверить, что в каждой строке только цифры
            result_uki_v_msisdn_files = []
            for uki_file in uki_files:
                df_uki = pd.read_csv(uki_dir + latest_uki_dir + uki_file, header=None, names=["UKI"])
                df_uki = pd.DataFrame(df_uki.iloc[:, 0])
                logger.debug("We choise first column in df_uki")
                for uki_errors in df_uki["UKI"].apply(
                        lambda x: len(str(x)) == 11 and str(x).startswith('1') and str(x).isdigit()):
                    if not uki_errors:
                        logger.error(
                            "In UKI-file lenght != 11 or string don't start with 1 or not just numbers in string")
                        sys.exit(3)

                # Объединение УКИ с УКИ и МСИСДН
                df_merged_uki = pd.merge(df_uki, concatenated_df_uki_and_msisdn).astype(str)
                logger.debug("merged UKI with UKI and MSISDN")

                df_merged_uki.loc[:, "UKI"] = "210006929285"
                logger.debug("Change UKI to LS")

                # Подготовка финального dataframe, который выгрузим в txt-файл
                df_merged_uki["UKI"] = df_merged_uki["UKI"].map(lambda x: x.replace(x[:], ';' + x[:] + ';'))
                df_merged_uki["MSISDN"] = df_merged_uki["MSISDN"].map(lambda x: x.replace(x[:], x[:] + ';;;'))
                df_merged_uki["all_data"] = df_merged_uki["UKI"].str.cat(df_merged_uki["MSISDN"], sep="")
                df_merged_uki = df_merged_uki.drop(columns=["UKI", "MSISDN"])
                logger.debug("Created final dataframe")

                # Теперь создадим название файла для ЛС и МСИСДН
                number_of_file = uki_file[uki_file.rfind('-'):uki_file.rfind('.')]
                merged_msisdn_file_name = f"VC-output-MSISDN-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + \
                                          number_of_file + ".txt"
                result_uki_v_msisdn_files.append(merged_msisdn_file_name)
                logger.debug(f"Created name for dataframe: {merged_msisdn_file_name}")

                # Теперь выгрузим файл
                df_merged_uki.to_csv(result_uki_v_msisdn_dir + '\\' + merged_msisdn_file_name, index=None, header=False)
                logger.debug("Uploaded the file")

            # Сообщение об окончании программы
            if flag:
                print(f"Результаты конвертации размещены в директории:\n"
                      f"{result_uki_v_msisdn_dir}"
                      f"Список файлов"
                      f"{', '.join(result_uki_v_msisdn_files)}\n"
                      f"Запомните или запишите на листочке папку, где сохранены файлы после конвертации.\n "
                      f"Программа успешно провела операцию конвертации\n"
                      f"и будет автоматически завершена через 30 секунд.")

                time.sleep(30)
                logger.debug("Program close")
                sys.exit(0)
            else:
                print(f"Конвертация уже была ранее проведена для входных файлов из папки:\n"
                      f"{uki_dir + latest_uki_dir}\n"
                      f"поэтому результаты этого запроса на конвертацию будут размещены в директории:\n"
                      f"{result_uki_v_msisdn_dir}\n"
                      f"Список файлов"
                      f"{', '.join(result_uki_v_msisdn_files)}\n"
                      f"Запомните или запишите на листочке папку, где сохранены файлы после конвертации.\n "
                      f"Программа успешно провела операцию конвертации\n"
                      f"и будет автоматически завершена через 30 секунд.\n")

                time.sleep(30)
                logger.debug("Program close")
                sys.exit(0)
        else:
            logger.debug("The employee could not enter the correct button the program was closed")
            sys.exit(9)


if __name__ == '__main__':
    logger.debug("Program started")
    convert_uki_to_msisdn()
