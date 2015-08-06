import pandas
from ipeds import read_with_column_subset, get_descriptions_subset


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
