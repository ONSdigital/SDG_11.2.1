import runpy

countries = ['eng_wales']

for country in countries:
    if country == 'eng_wales':
        print(f'Running pipeline for {country}')
        runpy.run_module('SDG_eng_wales', run_name='__main__')
