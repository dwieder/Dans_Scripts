from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np

pd.set_option('display.expand_frame_repr',False)

conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/magnus_apps')
conn_pgsql1 = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')


query = '''
Select a.scholar_id,  s.grade, a.achievement
FROM achievement a
INNER JOIN scholars s
        ON a.scholar_id=s.scholar_id
WHERE a.indicator = 'Academic Performance' AND a.reference_date = CURRENT_DATE;
'''

df1 = read_sql(query, conn_pgsql)


teachers = pd.ExcelFile('/Users/dwieder/Desktop/teachers.xls')
teachers

teachers.sheet_names


df2 = teachers.parse('Sheet1')

df3 = pd.merge(df1, df2, how='inner', on='scholar_id')

scholar = '''
SELECT
s.name,
sc.nickname,
p.first_name,
p.last_name,
ca.scholar_id
FROM class_assignment ca
INNER JOIN school_assignment sa
        ON ca.scholar_id=sa.scholar_id AND sa.academic_year_id=ca.academic_year_id
INNER JOIN grade_assignment ga
        ON ca.scholar_id=ga.scholar_id AND ga.academic_year_id=ca.academic_year_id
INNER JOIN school_class sc
        ON sc.id=ca.school_class_id AND ca.academic_year_id=sc.academic_year_id
INNER JOIN school s
        ON s.id=sc.school_id
INNER JOIN scholar ss
        ON ss.id=ca.scholar_id
INNER JOIN person p
        ON ca.scholar_id=p.id
WHERE ca.academic_year_id=395
AND ss.withdrawal_reason_id IS NULL
AND ca.end_date IS NULL
AND ga.end_date IS NULL
AND sc.subject_id IS NULL
GROUP BY s.name, ga.grade, sc.nickname, p.first_name, p.last_name, ca.scholar_id,s.abbreviation
ORDER BY ca.scholar_id;
'''

scholars = read_sql(scholar, conn_pgsql1)

df4 = pd.merge(df3, scholars, how='left', on='scholar_id')

df4['category'] = df4.groupby('grade')['achievement'].apply(lambda x: calculate_boxplot(x, categorize=True))

# df4.to_excel('list_of_scholar.xls')