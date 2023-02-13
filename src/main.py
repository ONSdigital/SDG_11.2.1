import SDG_ni
import SDG_eng_wales
import SDG_scotland

countries = ['eng_wales', 'scotland', 'northern_ireland']

for country in countries:
    if country == 'eng_wales':
        print(f'Running pipeline for {country}')
        SDG_eng_wales

    if country == 'scotland':
        print(f'Running pipeline for {country}')
        SDG_scotland

    if country == 'northern_ireland':
        print(f'Running pipeline for {country}')
        SDG_ni
