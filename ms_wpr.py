from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np

pd.set_option('display.expand_frame_repr',False)

conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')
conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')


##PULL CULTURE DATA
culture_query = '''
WITH scholar AS (
        SELECT
        regexp_replace(s.name, '^Success Academy ', '') AS school_name,
        regexp_replace(s.abbreviation, '^SA-', '') AS school,
        ga.grade,
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
        AND (ga.grade in (5,6,7) OR (s.id in (11405) AND ga.grade in (4)))
        AND ss.withdrawal_reason_id IS NULL
        AND ss.exit_date IS NULL
        AND ca.end_date IS NULL
        AND ga.end_date IS NULL
        AND sc.subject_id IS NULL
        GROUP BY s.name, ga.grade, sc.nickname, p.first_name, p.last_name, ca.scholar_id,s.abbreviation
        ORDER BY ca.scholar_id
), absent AS (
        SELECT scholar_id, COUNT(*) AS absent_count
        FROM absence
        INNER JOIN class_infractions ON absence.class_infractions_id = class_infractions.id
        WHERE class_infractions.date BETWEEN ('{Date1}'::date) and ('{Date2}'::date) AND excused = False
        GROUP BY scholar_id
), tardy AS (
        SELECT scholar_id, COUNT(*) AS tardy_count
        FROM tardy
        INNER JOIN class_infractions ON tardy.class_infractions_id = class_infractions.id
        WHERE class_infractions.date BETWEEN ('{Date1}'::date) and ('{Date2}'::date) AND excused = False
        GROUP BY scholar_id
), suspension AS (
        SELECT scholar_id, COUNT(*) AS suspension_count
        FROM suspension
        WHERE start_date BETWEEN ('{Date1}'::date) and ('{Date2}'::date)
        GROUP BY scholar_id
), uniform AS (
        SELECT scholar_id, COUNT(*) AS uniform_count
        FROM uniform
        INNER JOIN class_infractions ON uniform.class_infractions_id = class_infractions.id
        WHERE class_infractions.date BETWEEN ('{Date1}'::date) and ('{Date2}'::date)
        GROUP BY scholar_id
)
SELECT
s.scholar_id,
s.school_name,
s.school,
s.first_name,
s.last_name,
s.nickname AS homeroom,
s.grade,
CASE
        WHEN t.tardy_count IS NULL THEN 0 ELSE t.tardy_count
END AS Tardy,
CASE
        WHEN a.absent_count IS NULL THEN 0 ELSE a.absent_count
END AS Absent,
CASE
        WHEN sp.suspension_count IS NULL THEN 0 ELSE sp.suspension_count
END AS Suspensions,
CASE
        WHEN u.uniform_count IS NULL THEN 0 ELSE u.uniform_count
END AS Uniform
FROM scholar s
LEFT JOIN absent a
        ON a.scholar_id=s.scholar_id
LEFT JOIN tardy t
        ON t.scholar_id=s.scholar_id
LEFT JOIN suspension sp
        ON sp.scholar_id=s.scholar_id
LEFT JOIN uniform u
        ON u.scholar_id=s.scholar_id
ORDER BY s.scholar_id '''


culture = read_sql(culture_query.format(Date1='2016-01-31', Date2='2016-02-13'), conn_pgsql)



reading = '''
SELECT
scholarid as "scholar_id",
SUM(achievedunits) AS "Reading Log Minutes"
FROM scholarweeklygoal wk
WHERE wk.goaltype = 1
AND (wk.goalyear = 2016 AND (wk.goalweek in (6,7)))
GROUP BY wk.scholarid
'''
reading_log = read_sql(reading,conn_mssql)


culture = pd.merge(culture, reading_log, how='inner', on='scholar_id')

culture['Reading Log Minutes'] = culture['Reading Log Minutes'].replace('NaN', '')

#Pull Primary Contact Emails and seperate into 2 columns
primary_contact_query = '''
SELECT
scholar_id,
first_name,
last_name,
CASE
    WHEN email like '%%noemail.successacademies.org%%' THEN ''
    ELSE email
END as email
FROM
    (SELECT
        sc.scholar_id,
        p.first_name,
        p.last_name,
        LOWER(p.email) as email
    FROM scholar_contact AS sc
    LEFT JOIN person AS p
        ON p.id = sc.contact_id
    WHERE (sc.is_main_contact = TRUE OR sc.contact_type=1)
    AND (p.email IS NOT NULL AND p.email<>'')
    GROUP BY sc.scholar_id,p.email, p.first_name, p.last_name
    ORDER BY sc.scholar_id ) AS contact
GROUP BY scholar_id, email, first_name, last_name
ORDER BY scholar_id'''


contact_df = pd.read_sql_query(primary_contact_query, conn_pgsql)

df = pd.DataFrame(columns=['scholar_id', 'email_1',  'email_2'])

for scholar in np.unique(contact_df.scholar_id):
    sub = contact_df.loc[contact_df.scholar_id.eq(scholar)]
    if sub.shape[0] == 1:
        tmp = pd.DataFrame(data={'scholar_id': scholar,  'email_1': sub.email.iloc[0], 'email_2': ''},
                           index=[0], columns=['scholar_id', 'email_1', 'email_2'])
    elif sub.shape[0] > 1:
        tmp = pd.DataFrame(data={'scholar_id': scholar,  'email_1': sub.iloc[0].email, 'email_2': sub.iloc[1].email},
                   index=[0], columns=['scholar_id', 'email_1',  'email_2'])
    df = df.append(tmp, ignore_index=True)
print 'done'

df.email_2 = ['' if x==df.email_1.iloc[i] else x for i, x in enumerate(df.email_2)]
df_contacts = df.fillna('')

#Merge Culture data with Contact data
culture_final = pd.merge(culture, df_contacts, how='left', on='scholar_id')



###Academics

##Curves


assessment_score_query = '''
SELECT
        scholar_id,
        CASE
                WHEN percent IS NULL THEN -1
                ELSE percent
        END as percent,
        CASE
                WHEN points IS NULL THEN -1
                ELSE points
        END as points,
        grade,
        CONCAT(a_name,' ','percent') as a_name
FROM
        (SELECT right(a.name,-4) AS "a_name",
               sa.scholar_id,
               sa.points,
               (ROUND(CAST((sa.points/a.points_possible) AS Numeric), 2)*100) AS percent,
               scholar.current_grade AS "grade"
        FROM scholar_assessment sa
        INNER JOIN assessment a
            ON a.id = sa.assessment_id
        INNER JOIN scholar
            ON scholar.id = sa.scholar_id
        WHERE
        a.id IN (3054,3056,3057,3087,3088,3089,3067,3068,3069)
        AND scholar.exit_date IS NULL) as score
'''

# run sql query and assign to a data frame
assessment_score = pd.read_sql_query(assessment_score_query, conn_pgsql)


assessment_score.to_excel('assessment_score.xls')

files = pd.ExcelFile('/Users/dwieder/Desktop/MS/assessment_score.xls')
files

files.sheet_names

basic_assessment_df = files.parse('Sheet1')


math_unit5_6_5_curve = {0: 0, 1: 17.25, 2: 34.5, 3: 51.75, 4: 69, 5: 70, 6: 72.25, 7: 74.5, 8: 76.75, 9: 79, 10: 80, 11:83,
                        12:86, 13: 89, 14: 90, 15: 95, 16: 100}
math_unit5_6_6_curve = {0: 0, 1: 13.8, 2: 27.6, 3: 41.4, 4: 55.2, 5: 69, 6: 70, 7: 72.25, 8: 74.5, 9: 76.74, 10: 79, 11: 80,
                        12: 84.5, 13: 89, 14: 90, 15: 93.33, 16:96.67, 17:100}
math_unit5_6_7_curve = {0: 0, 1: 17.25, 2: 34.5, 3: 51.75, 4: 69, 5: 70, 6: 73, 7: 76, 8: 79, 9: 80, 10: 84.5,
                         11: 89, 12: 95}

# curve any needed Math Assessments
math_assessments_to_map = ['Unit 5-6 Math Test 2.2.16 percent']
assessment_filter = basic_assessment_df['a_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(5)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_5_curve)

assessment_filter = basic_assessment_df['a_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(6)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_6_curve)

assessment_filter = basic_assessment_df['a_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(7)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_7_curve)


science_unit_5_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 29, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
                        12: 69, 13: 71, 14: 72, 15: 74, 16: 75, 17: 76, 18: 77, 19: 78, 20: 79, 21: 81, 22: 82, 23: 84,
                        24: 86, 25: 87, 26: 89, 27: 91, 28 : 92, 29: 94, 30 : 96, 31 : 97, 32 : 98, 33:99, 34: 100 }

science_unit_6_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 28, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
                        12: 69, 13: 70, 14: 71, 15: 72, 16: 73, 17: 74, 18: 75, 19: 76, 20: 77, 21: 78, 22: 79, 23: 80,
                        24: 81, 25: 82, 26: 83, 27: 85, 26:83, 27:85, 28:86, 29: 87, 30:88, 31:89, 32:91, 33:92, 34:93, 35:95, 36:96, 37:97,
                        38:98, 39:99, 40: 100}

science_unit_7_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 28, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
                        12: 69, 13: 70, 14: 71, 15: 72, 16: 73, 17: 74, 18: 75, 19: 76, 20: 77, 21: 78, 22: 79, 23: 80,
                        24: 81, 25: 82, 26: 83, 27: 85, 26:83, 27:85, 28:86, 29: 87, 30:88, 31:89, 32:91, 33:92, 34:93, 35:95, 36:96, 37:97,
                        38:98, 39:99, 40: 100}

# curve any needed Science Assessments
science_assessments_to_map = ['Science Unit 4 & 5 Test 2.4.16 percent']
assessment_filter = basic_assessment_df['a_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(5)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_5_curve)
assessment_filter = basic_assessment_df['a_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(6)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_6_curve)
assessment_filter = basic_assessment_df['a_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(7)
basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_7_curve)

basic_assessment_df.loc[basic_assessment_df.a_name.eq('Science Unit 4 & 5 Test 2.4.16 percent')]


assessment_score = pd.pivot_table(basic_assessment_df, values='percent', index=['scholar_id'], columns=['a_name'])

assessment_score = assessment_score.replace(-1, 'N/A')

assessment_score = assessment_score.fillna('')

assessment_score_final = assessment_score.reset_index()

assessment_score_final.columns



#Get Mastery Levels for Assessments
assessment_mastery_query = '''
SELECT
        CASE
                WHEN mastery_level IS NULL THEN -1
                ELSE mastery_level
        END as mastery_level,
        a_name,
        scholar_id
FROM
        (SELECT right(a.name,-4) AS "a_name",
               sa.scholar_id,
               sa.mastery_level
        FROM scholar_assessment sa
        INNER JOIN assessment a
            ON a.id = sa.assessment_id
        INNER JOIN scholar
            ON scholar.id = sa.scholar_id
        WHERE a.id IN (3054,3056,3057,3087,3088,3089,3067,3068,3069)
        AND scholar.exit_date IS NULL) as mastery
'''


assessment_mastery = pd.read_sql_query(assessment_mastery_query, conn_pgsql)

assessment_mastery.to_excel('assessment_mastery.xls')

files = pd.ExcelFile('/Users/dwieder/Desktop/MS/assessment_mastery.xls')
files

files.sheet_names

assessment_mastery = files.parse('Sheet1')

assessment_mastery.loc[assessment_mastery.scholar_id.eq(3472)]


assessment_mastery_levels = pd.pivot_table(assessment_mastery, values='mastery_level', index=['scholar_id'], columns=['a_name'])

assessment_mastery_level_final = assessment_mastery_levels.replace([1,2,3,4], ['Below Expectations', 'Approaching Expectations', 'Meeting Expectations', 'Exceeding Expectations' ])

assessment_mastery_final= assessment_mastery_level_final.fillna('')

assessment_mastery_final = assessment_mastery_final.replace(-1, 'N/A')

assessment_mastery_final = assessment_mastery_final.reset_index()



#merge assessments
assessments = pd.merge(assessment_score_final,assessment_mastery_final, how='inner', on='scholar_id')

assessments.columns

assessments.columns.name=None

#Re-arrange columns to pair assessments by mastery and percent levels
assessment_final = assessments.iloc[:,[0 , 1 , 6 , 2 , 7, 3 , 8, 4 , 9, 5, 10]]

#Merge culture and assessment
final = pd.merge(culture_final, assessment_final, how='left', on='scholar_id').set_index('scholar_id')



final.to_excel('MS WPR merge 2.18.16.xls')
