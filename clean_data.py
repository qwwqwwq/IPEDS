import pandas
import os.path

FILES = ["effy2013",
         "hd2013",
         "ic2013_ay",
         "sfa1213"]

def get_names(data_name):
    if os.path.exists(data_name + ".xlsx"):
        return pandas.read_excel(data_name + ".xlsx", sheetname=1).varTitle.tolist()
    elif os.path.exists(data_name + ".xls"):
        return pandas.read_excel(data_name + ".xls", sheetname=1).varTitle.tolist()
    else:
        raise Exception()

def get_inclusion(data_name):
    ext = None
    if os.path.exists(data_name + ".xlsx"):
        ext = ".xlsx"
    elif os.path.exists(data_name + ".xls"):
        ext = ".xls"
    else:
        raise Exception()
    return pandas.read_excel(data_name + ext, sheetname=1).varname.tolist()

def main():
    joined = None
    for data_name in FILES:
        header = get_names(data_name)
        include = get_inclusion(data_name)
        convert = { k:v for (k,v) in zip(include, header) }
        df = pandas.read_csv(data_name + ".csv", index_col=0, usecols=include)
        df.rename(columns=convert, inplace=True)
        import ipdb; ipdb.set_trace()
        if joined is None:
            joined = df
        else:
            joined = joined.join(df)

    joined.to_csv("joined.csv")



if __name__ == '__main__':
    main()
