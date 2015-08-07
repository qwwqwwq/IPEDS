from ipeds import read_with_column_subset

def main():
    institutional_basic_info = read_with_column_subset("hd2013", ['INSTNM', 'LONGITUD', 'LATITUDE', 'UGOFFER', 'HLOFFER', 'INSTSIZE'])
    institutional_basic_info = institutional_basic_info[(institutional_basic_info['Highest level of offering'] >= 5) & (institutional_basic_info['Undergraduate offering'] == 1)]
    del institutional_basic_info['Highest level of offering']
    del institutional_basic_info['Undergraduate offering']

    institutional_basic_info.dropna().to_csv("map_dataset.csv", header=False)

if __name__ == '__main__':
    main()
