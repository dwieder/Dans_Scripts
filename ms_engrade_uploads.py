import pandas as pd
import sqlalchemy
import numpy as np
import re

conn_postgres = sqlalchemy.create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')
conn_postgres.connect()
conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')

# get assessment scores for assessments that have no curve or are curved based on overall percent and mastery levels
basic_assessment_query = '''
WITH homeroom AS (
    SELECT
    ca3.scholar_id,
    sc3.nickname AS name
    FROM class_assignment AS ca3
    INNER JOIN school_class AS sc3
        ON ca3.school_class_id = sc3.id
    WHERE ca3.subject_id::int IS NULL
    AND '2016-01-08'::date BETWEEN ca3.start_date AND COALESCE(ca3.end_date, CURRENT_DATE)
)
SELECT
right(a.name,-4) AS "assessment_name",
a.id AS "assessment_id",
regexp_replace(sh.name, '^Success Academy ', '') AS "school_name",
regexp_replace(sh.abbreviation, '^SA-', '') AS "school",
regexp_replace(sc.nickname, '/', '-') AS "class_name",
sc.id AS "class_id",
p.first_name,
p.last_name,
subj.name AS "subject",
sa.scholar_id,
current_grade AS grade,
sa.assessment_date,
sa.points,
a.points_possible,
(sa.points/a.points_possible * 100) AS "percent"
FROM scholar_assessment sa
INNER JOIN assessment a
    ON a.id = sa.assessment_id
INNER JOIN assessment_type at
    ON at.id = a.assessment_type_id
INNER JOIN subject subj
    ON at.subject_id = subj.id
INNER JOIN scholar
    ON scholar.id = sa.scholar_id
INNER JOIN school_class sc
    ON sc.id = sa.school_class_id
INNER JOIN school sh
    ON sh.id = sc.school_id
INNER JOIN person p
    ON p.id = sa.scholar_id
INNER JOIN homeroom hr
    ON hr.scholar_id = sa.scholar_id
WHERE
a.grade >= 5
AND a.grade <= 7
AND scholar.exit_date IS NULL
AND a.id IN (3087,3088,3089)
ORDER BY scholar.id, sa.assessment_id, sh.name
'''

# run sql query and assign to a data frame
basic_assessment_df = pd.read_sql_query(basic_assessment_query, conn_postgres)



# remove decimals from percent column
basic_assessment_df['percent'] = np.round(basic_assessment_df['percent'], decimals=0)

# create ELA curve dictionary
# ela_map = {0: 0, 25: 35, 50: 60, 75: 85, 100: 100}
#
# # curve any needed History assessments
# assessments_to_map = ['Unit 3 PUP']
# assessment_filter = basic_assessment_df['assessment_name'].isin(assessments_to_map)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'percent'].map(ela_map)


# math_unit5_6_5_curve = {0: 0, 1: 17.25, 2: 34.5, 3: 51.75, 4: 69, 5: 70, 6: 72.25, 7: 74.5, 8: 76.75, 9: 79, 10: 80, 11:83,
#                         12:86, 13: 89, 14: 90, 15: 95, 16: 100}
# math_unit5_6_6_curve = {0: 0, 1: 13.8, 2: 27.6, 3: 41.4, 4: 55.2, 5: 69, 6: 70, 7: 72.25, 8: 74.5, 9: 76.74, 10: 79, 11: 80,
#                         12: 84.5, 13: 89, 14: 90, 15: 93.33, 16:96.67, 17:100}
# math_unit5_6_7_curve = {0: 0, 1: 17.25, 2: 34.5, 3: 51.75, 4: 69, 5: 70, 6: 73, 7: 76, 8: 79, 9: 80, 10: 84.5,
#                          11: 89, 12: 95}
#
# # curve any needed Math Assessments
# math_assessments_to_map = ['Unit 5-6 Math Test 2.2.16']
# assessment_filter = basic_assessment_df['assessment_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(5)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_5_curve)
#
# assessment_filter = basic_assessment_df['assessment_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(6)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_6_curve)
#
# assessment_filter = basic_assessment_df['assessment_name'].isin(math_assessments_to_map) & basic_assessment_df['grade'].eq(7)
# # basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(math_unit5_6_7_curve)
#
#
# science_unit_5_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 29, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
#                         12: 69, 13: 71, 14: 72, 15: 74, 16: 75, 17: 76, 18: 77, 19: 78, 20: 79, 21: 81, 22: 82, 23: 84,
#                         24: 86, 25: 87, 26: 89, 27: 91, 28 : 92, 29: 94, 30 : 96, 31 : 97, 32 : 98, 33:99, 34: 100 }
#
# science_unit_6_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 28, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
#                         12: 69, 13: 70, 14: 71, 15: 72, 16: 73, 17: 74, 18: 75, 19: 76, 20: 77, 21: 78, 22: 79, 23: 80,
#                         24: 81, 25: 82, 26: 83, 27: 85, 26:83, 27:85, 28:86, 29: 87, 30:88, 31:89, 32:91, 33:92, 34:93, 35:95, 36:96, 37:97,
#                         38:98, 39:99, 40: 100}
#
# science_unit_7_curve = {0: 0, 1: 6, 2: 12, 3: 17, 4: 23, 5: 28, 6: 35, 7: 40, 8: 46, 9: 52, 10: 58, 11: 63,
#                         12: 69, 13: 70, 14: 71, 15: 72, 16: 73, 17: 74, 18: 75, 19: 76, 20: 77, 21: 78, 22: 79, 23: 80,
#                         24: 81, 25: 82, 26: 83, 27: 85, 26:83, 27:85, 28:86, 29: 87, 30:88, 31:89, 32:91, 33:92, 34:93, 35:95, 36:96, 37:97,
#                         38:98, 39:99, 40: 100}
#
# # curve any needed Science Assessments
# science_assessments_to_map = ['Science Unit 4 & 5 Test 2.4.16']
# assessment_filter = basic_assessment_df['assessment_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(5)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_5_curve)
# assessment_filter = basic_assessment_df['assessment_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(6)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_6_curve)
# assessment_filter = basic_assessment_df['assessment_name'].isin(science_assessments_to_map) & basic_assessment_df['grade'].eq(7)
# basic_assessment_df.loc[assessment_filter, 'percent'] = basic_assessment_df.loc[assessment_filter, 'points'].map(science_unit_7_curve)









# get list of assessment ids to loop through
assessments = basic_assessment_df['assessment_id'].unique()
assessments


# loop through assessment ids and generate an engrade upload csv file
for a in assessments:
   y = basic_assessment_df[basic_assessment_df['assessment_id'].eq(a)]
   classes = y['class_id'].unique()
   name = y['assessment_name'].unique()
   date = y['assessment_date'].unique()
   subject = y['subject'].unique()
   grade = y['grade'].unique()
   if subject[0] == 'Literacy':
       type = 'Unit Test/Project/PUP'
   elif subject[0] == 'History':
       type = 'Vocab Quiz'
   elif subject[0] == 'Mathematics':
       type = 'NHM Quiz'
   elif subject[0] == 'Science':
       type = 'Unit Test/Project'
   else:
       type = 'Unit Test/Project'

   for c in classes:
       print(c)
       df2 = y[y['class_id'].eq(c)]
       class_name = df2['class_name'].unique()
       print(class_name[0])
       school = df2['school'].unique()
       print(school[0])

       df = pd.DataFrame(columns=range(6))
       df.loc[0, :] = school[0], '', '', '', '', ''
       df.loc[1, :] = 'First', 'Last', 'ID', 'Grade', 'Percent', name[0]
       df.loc[2, :] = '', '', '', '', '', type
       df.loc[3, :] = '', '', '', '', '', date[0].strftime('%Y%m%d')
       df.loc[4, :] = '', '', '', '', '', 100
       df.loc[5, :] = '', '', '', '', '', ''


       df2 = df2.\
           rename(columns={'first_name': 0, 'last_name': 1, 'scholar_id': 2, 'percent': 5}).\
           reindex_axis(range(6), axis=1).\
           fillna('')

       df = df.append(df2)
       df.reset_index()
       df.convert_objects(convert_numeric=True)

       output_string = school[0] + '_' + class_name[0] + '_' + subject[0] + '_' + 'G' + str(grade[0]) + '_' + name + '.csv'
       df.to_csv('/Users/dwieder/Google Drive/MS_Engrade_Uploads/2.16.16/' \
                 + school[0] + '/' + output_string[0], header=False, index=False)






# pull reading logs infractions
reading_logs_query = '''
SELECT scholar_id,
    goal_year,
    goal_week,
    achieved_units,
    goal_units,
    achieved_units >= goal_units AS goal
FROM scholar_weekly_goal wk
WHERE wk.goal_type = 1
AND (wk.goal_year = 2016 AND (wk.goal_week in (6,7)))
GROUP BY wk.scholar_id, wk.goal_year, wk.goal_week, wk.achieved_units, wk.goal_units
'''

reading_logs_df = pd.read_sql_query(reading_logs_query, conn_postgres)

reading = '''
SELECT
    scholarid as "scholar_id",
    goalyear,
    goalweek,
    achievedunits AS "achieved_units",
    goalunits
FROM scholarweeklygoal
WHERE goaltype = 1
AND (goalyear = 2016 AND (goalweek in (6,7)))
GROUP BY scholarid,goalyear, goalweek,achievedunits, goalunits
'''
reading_logs_df = read_sql(reading,conn_mssql)



# group achieved units (minutes read) by scholar_id
rl_minutes_read = reading_logs_df.groupby(['scholar_id'])['achieved_units'].sum().reset_index()


# get list of scholars and homeroom
homeroom_query = '''
SELECT
ca.scholar_id,
regexp_replace(s.name, '^Success Academy ', '') AS school_name,
regexp_replace(s.abbreviation, '^SA-', '') AS school,
p.first_name,
p.last_name,
sc.nickname AS homeroom,
ga.grade
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
AND ga.grade in (5,6,7)
AND ss.withdrawal_reason_id IS NULL
AND ca.end_date IS NULL
AND ga.end_date IS NULL
AND sc.subject_id IS NULL
GROUP BY s.name, ga.grade, sc.nickname, p.first_name, p.last_name, ca.scholar_id,s.abbreviation
ORDER BY ca.scholar_id
'''

homeroom_df = pd.read_sql_query(homeroom_query, conn_postgres)

# merge reading logs dataframe with homeroom dataframe
reading_logs_df = pd.DataFrame()
reading_logs_df = pd.merge(homeroom_df, rl_minutes_read, how='left', on='scholar_id')


# loop through homeroom classes and generate an engrade upload csv file for reading logs
classes = reading_logs_df['homeroom'].unique()
name = 'RL 1/31-2/13/16'
date = '20160219'
subject = 'ACTION NOW'
type = 'Independent Reading'

for c in classes:
    class_name = re.sub(r'\W+', '', c)
    print(class_name)
    df = pd.DataFrame(columns=range(6))
    df.loc[0, :] = c, '', '', '', '', ''
    df.loc[1, :] = 'First', 'Last', 'ID', 'Grade', 'Percent', name
    df.loc[2, :] = '', '', '', '', '', type
    df.loc[3, :] = '', '', '', '', '', date
    df.loc[4, :] = '', '', '', '', '', 540
    df.loc[5, :] = '', '', '', '', '', ''

    df2 = reading_logs_df[reading_logs_df['homeroom'].eq(c)]
    school = df2['school'].unique()

    df2 = df2.\
        rename(columns={'first_name': 0, 'last_name': 1, 'scholar_id': 2, 'achieved_units': 5}).\
        reindex_axis(range(6), axis=1).\
        fillna('')

    df = df.append(df2)
    df.reset_index()
    df.convert_objects(convert_numeric=True)

    output_string = school[0] + '_' + class_name + '_' + subject + '_' + date + '.csv'
    print(output_string)
    df.to_csv('/Users/dwieder/Google Drive/MS_Engrade_Uploads/2.16.16/'
              + school[0] + '/' + output_string, header=False, index=False)