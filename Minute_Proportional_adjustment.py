import pandas as pd
import os.path
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import warnings
plt.style.use('seaborn-darkgrid')
warnings.filterwarnings('ignore')

year_begining = 2010
year_end = 2023  # should be +1 of what you need

path_with_gz = "D:/Git_Work/peter-model/fut_taq_1min/2010/20100103/ES/"
iden_month = {'F': '1', 'G': '2', 'H': '3', 'J': '4', 'K': '5', 'M': '6', 'N': '7', 'Q': '8', 'U': '9', 'V': '10',
              'X': '11', 'Z': '12'}
file_names = ["ESH0.csv.gz", "ESM0.csv.gz", "ESU0.csv.gz", "ESZ0.csv.gz",
              "ESH1.csv.gz", "ESM1.csv.gz", "ESU1.csv.gz", "ESZ1.csv.gz",
              "ESH2.csv.gz", "ESM2.csv.gz", "ESU2.csv.gz", "ESZ2.csv.gz",
              "ESH3.csv.gz", "ESM3.csv.gz", "ESU3.csv.gz", "ESZ3.csv.gz",
              "ESH4.csv.gz", "ESM4.csv.gz", "ESU4.csv.gz", "ESZ4.csv.gz"]


def look_if_end_data(y, m, d):
    if y == year_end - 1:
        if m == 12:
            if d == 31:
                return False
    return True

# создать поиск года к которому относится контракт чтоб вернуть его в yield.


def find_year_for_file(file_name_find, cur_year):
    number_in_file = int(file_name_find[3])
    years_for_file = []
    for y in range(year_begining, year_end):
        if y % 10 == number_in_file:
            years_for_file.append(y)
    years_for_file.append(years_for_file[-1]+10)
    left_side = 0
    right_side = years_for_file[0]
    for count, y in enumerate(years_for_file):
        if (left_side < cur_year) and (right_side >= cur_year):
            return y
        left_side = y
        if count + 1 >= len(years_for_file):
            right_side = year_end+1000
        else:
            right_side = years_for_file[count+1]


def find_path(path, name_file):
    year_to_replace = str(2010)
    for year in range(year_begining, year_end):
        path = path.replace(year_to_replace, str(year))
        year_to_replace = str(year)
        for month in range(1, 13):
            if month < 10:
                path = path[:46] + str(0)+str(month) + path[48:]
            else:
                path = path[:46]+str(month)+path[48:]
            for day in range(1, 32):
                if day < 10:
                    path = path[:48]+str(0)+str(day)+path[50:]
                else:
                    path = path[:48] + str(day) + path[50:]
                not_end = look_if_end_data(year, month, day)
                if not os.path.exists(path+name_file):
                    if not not_end:
                        yield "", not_end, year
                    print(f"file={name_file} does not exist for year={year} month={month} day={day} ")
                    continue
                yield path, not_end, find_year_for_file(name_file, year)


def find_all_data_for_contract():
    data = {}
    for num, file_name in enumerate(file_names):
        cur_data = True
        data[file_name[:4]] = {}
        while cur_data:
            for down_path, cur_data, data_year in find_path(path_with_gz, file_name):
                if down_path is not "":
                    if data_year not in data[file_name[:4]].keys():
                        data[file_name[:4]][data_year] = pd.DataFrame()
                    d = data[file_name[:4]][data_year]
                    data[file_name[:4]][data_year] = d.append(pd.read_csv(down_path + file_name, index_col=1,
                                                                          parse_dates=[['UTCDate',
                                                                                        'UTCTimeBarStart'],
                                                                                       ['LocalDate',
                                                                                        'LocalTimeBarStart']]))
    key_for_del = []
    for key in data:
        if len(data[key]) == 0:
            key_for_del.append(key)
    for k in key_for_del:
        del data[k]

    for f in file_names:
        for k in list(data[f[:4]].keys()):
            data[f[:4]][k].CloseBidPrice.fillna(method='ffill', inplace=True)
            data[f[:4]][k].CloseAskPrice.fillna(method='ffill', inplace=True)
            data[f[:4]][k]["Settle"] = (data[f[:4]][k].CloseBidPrice+data[f[:4]][k].CloseAskPrice)/2
    return data


def proportional_adjustment():
    all_data = find_all_data_for_contract()
    # Define the plot size
    plt.figure(figsize=(15, 7))
    plt.title('Unadjusted Futures Contracts', fontsize=14)
    plt.xlabel('Year-Month', fontsize=12)
    plt.ylabel('Price', fontsize=12)

    # Plotting the first contract
    for f in file_names:
        k_1 = list(all_data[f[:4]].keys())
        plt.plot(all_data[f[:4]][k_1[0]].Settle, label=f[:4], cmap='hsv')

    # Adding the legend
    plt.legend()
    # Show the plot
    plt.show()


proportional_adjustment()
