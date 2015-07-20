import pandas
import os.path
import numpy 

FILES = ["effy2013",
         "hd2013",
         "ic2013_ay",
         "sfa1213"]

rel_map = {-2: 'Not applicable',
 22: 'American Evangelical Lutheran Church',
 24: 'African Methodist Episcopal Zion Church',
 27: 'Assemblies of God Church',
 28: 'Brethren Church',
 30: 'Roman Catholic',
 33: 'Wisconsin Evangelical Lutheran Synod',
 34: 'Christ and Missionary Alliance Church',
 35: 'Christian Reformed Church',
 36: 'Evangelical Congregational Church',
 37: 'Evangelical Covenant Church of America',
 38: 'Evangelical Free Church of America',
 39: 'Evangelical Lutheran Church',
 40: 'International United Pentecostal Church',
 41: 'Free Will Baptist Church',
 42: 'Interdenominational',
 43: 'Mennonite Brethren Church',
 44: 'Moravian Church',
 45: 'North American Baptist',
 47: 'Pentecostal Holiness Church',
 48: 'Christian Churches and Churches of Christ',
 49: 'Reformed Church in America',
 50: 'Episcopal Church Reformed',
 51: 'African Methodist Episcopal',
 52: 'American Baptist',
 54: 'Baptist',
 55: 'Christian Methodist Episcopal',
 57: 'Church of God',
 58: 'Church of Brethren',
 59: 'Church of the Nazarene',
 60: 'Cumberland Presbyterian',
 61: 'Christian Church (Disciples of Christ)',
 64: 'Free Methodist',
 65: 'Friends',
 66: 'Presbyterian Church (USA)',
 67: 'Lutheran Church in America',
 68: 'Lutheran Church - Missouri Synod',
 69: 'Mennonite Church',
 71: 'United Methodist',
 73: 'Protestant Episcopal',
 74: 'Churches of Christ',
 75: 'Southern Baptist',
 76: 'United Church of Christ',
 77: 'Protestant not specified',
 78: 'Multiple Protestant Denomination',
 79: 'Other Protestant',
 80: 'Jewish',
 81: 'Reformed Presbyterian Church',
 84: 'United Brethren Church',
 87: 'Missionary Church Inc',
 88: 'Undenominational',
 89: 'Wesleyan',
 91: 'Greek Orthodox',
 92: 'Russian Orthodox',
 93: 'Unitarian Universalist',
 94: 'Latter Day Saints (Mormon Church)',
 95: 'Seventh Day Adventists',
 97: 'The Presbyterian Church in America',
 99: 'Other (none of the above)',
 100: 'Original Free Will Baptist',
 102: 'Evangelical Christian',
 103: 'Presbyterian'}

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

def get_descriptions(data_name):
    ext = None
    if os.path.exists(data_name + ".xlsx"):
        ext = ".xlsx"
    elif os.path.exists(data_name + ".xls"):
        ext = ".xls"
    else:
        raise Exception()
    return pandas.read_excel(data_name + ext, sheetname=2)

def read_with_column_subset(data_name, subset):
    header = get_names(data_name)
    include = get_inclusion(data_name)
    convert = { k:v for (k,v) in zip(include, header) }
    df = pandas.read_csv(data_name + ".csv", index_col=0, usecols=(['UNITID'] + subset), na_values = [".", " ", "", "-2"])
    df.rename(columns=convert, inplace=True)
    return df

def get_descriptions_subset(data_name, subset):
    header = get_names(data_name)
    include = get_inclusion(data_name)
    convert = { k:v for (k,v) in zip(include, header) }
    desc_df = get_descriptions(data_name)
    desc_df = desc_df[ desc_df.varname.apply(lambda x: x in subset) ]
    desc_df.varname = desc_df.varname.apply(lambda x: convert[x])
    return desc_df

def main():
    desc = []
    institutional_characteristics = read_with_column_subset("ic2013", ['ADMSSN', 'APPLCN', 'SPORT1', 'ENRLM', 'SATVR25', 'SATMT25', 'SATWR25', 'RELAFFIL'])
    institutional_characteristics['Religious affiliation'] =institutional_characteristics['Religious affiliation'].apply(lambda x: rel_map.get(x, 'None'))
    desc.append( get_descriptions_subset("ic2013", ['ADMSSN', 'APPLCN', 'SPORT1', 'ENRLM', 'SATVR25', 'SATMT25', 'SATWR25', 'RELAFFIL']))
    institutional_characteristics['SAT overall 25th percentile score'] = institutional_characteristics['SAT Critical Reading 25th percentile score'] + institutional_characteristics['SAT Math 25th percentile score'] + institutional_characteristics['SAT Writing 25th percentile score']
    del institutional_characteristics['SAT Critical Reading 25th percentile score']
    del institutional_characteristics['SAT Math 25th percentile score']
    del institutional_characteristics['SAT Writing 25th percentile score']
    institutional_basic_info = read_with_column_subset("hd2013", ['INSTNM', 'STABBR', 'LONGITUD', 'LATITUDE', 'CBSA', 'UGOFFER', 'HLOFFER'])
    desc.append(get_descriptions_subset("hd2013", ['INSTNM', 'STABBR', 'LONGITUD', 'LATITUDE', 'CBSA', 'UGOFFER', 'HLOFFER']))
    institutional_basic_info = institutional_basic_info[(institutional_basic_info['Highest level of offering'] >= 5) & (institutional_basic_info['Undergraduate offering'] == 1)]
    del institutional_basic_info['Undergraduate offering']
    del institutional_basic_info['Highest level of offering']
    demographics = read_with_column_subset("effy2013", ['EFFYLEV', 'EFYTOTLT', 'EFYTOTLM', 'EFYTOTLW', 'EFYASIAT', 'EFYBKAAT', 'EFYHISPT', 'EFYNHPIT', 'EFYWHITT'])
    desc.append(get_descriptions_subset("effy2013", ['EFFYLEV', 'EFYTOTLT', 'EFYTOTLM', 'EFYTOTLW', 'EFYASIAT', 'EFYBKAAT', 'EFYHISPT', 'EFYNHPIT', 'EFYWHITT']))
    demographics = demographics[demographics[u'Level of student'] == 1]
    del demographics[u'Level of student']
    tuition = read_with_column_subset("ic2013_ay", ['TUITION1', 'TUITION2', 'TUITION3', 'CHG4AY3', 'CHG5AY3'])
    desc.append(get_descriptions_subset("ic2013_ay", ['TUITION1', 'TUITION2', 'TUITION3', 'CHG4AY3', 'CHG5AY3']))
    completions = read_with_column_subset("c2013_b", ['CSTOTLT', 'CSTOTLM', 'CSTOTLW'])
    desc.append(get_descriptions_subset("c2013_b", ['CSTOTLT', 'CSTOTLM', 'CSTOTLW']))
    salaries_and_staff = read_with_column_subset("sal2013_is", ['SATOTLT', 'SAOUTLT']).groupby(level=0).sum()
    desc.append(get_descriptions_subset("sal2013_is", ['SATOTLT', 'SAOUTLT']))
    salaries_and_staff['Average yearly instructor compensation'] = numpy.round(salaries_and_staff['Salary outlays - total'] / salaries_and_staff['Instructional staff on 9, 10, 11 or 12 month contract-total'], 2)

    financial_aid = read_with_column_subset("sfa1213", ['UAGRNTP', 'UAGRNTA', 'UFLOANA', 'UFLOANP'])
    desc.append(get_descriptions_subset("sfa1213", ['UAGRNTP', 'UAGRNTA', 'UFLOANA', 'UFLOANP']))

    output = institutional_basic_info.join(institutional_characteristics)
    output = output.join(demographics)
    output = output.join(tuition)
    output = output.join(completions, rsuffix=' Graduated')
    output = output.join(salaries_and_staff)
    output = output.join(financial_aid)
    output = output.dropna()
    output.to_csv("ipeds_dataset.csv")
    desc_df = pandas.concat(desc)
    del desc_df['varnumber']
    desc_df.to_csv("ipeds_descriptions.csv", encoding='utf-8', index=False)

if __name__ == '__main__':
    main()
