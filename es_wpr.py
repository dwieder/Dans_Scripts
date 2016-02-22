
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
        AND ga.grade in (0,1,2,3,4)
        AND ss.withdrawal_reason_id IS NULL
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
), contact AS (
        SELECT
        scholar_id,
        CASE
            WHEN email like '%%noemail.successacademies.org%%' THEN ''
            ELSE email
        END as email
        FROM
            (SELECT
                sc.scholar_id,
                LOWER(p.email) as email
            FROM scholar_contact AS sc
            LEFT JOIN person AS p
            ON p.id = sc.contact_id
            WHERE (sc.is_main_contact = TRUE OR sc.contact_type=1)
            AND (p.email IS NOT NULL AND p.email<>'')
            GROUP BY sc.scholar_id,p.email
            ORDER BY sc.scholar_id ) AS contact
        GROUP BY scholar_id, email
        ORDER BY scholar_id
)
SELECT
s.scholar_id,
s.nickname AS homeroom,
s.school_name,
s.school,
s.grade,
s.first_name,
s.last_name,
CASE
        WHEN a.absent_count IS NULL THEN 0 ELSE a.absent_count
END AS Absent,
CASE
        WHEN t.tardy_count IS NULL THEN 0 ELSE t.tardy_count
END AS Tardy,
CASE
        WHEN sp.suspension_count IS NULL THEN 0 ELSE sp.suspension_count
END AS Suspensions,
c.email AS Email
FROM scholar s
LEFT JOIN absent a
        ON a.scholar_id=s.scholar_id
LEFT JOIN tardy t
        ON t.scholar_id=s.scholar_id
LEFT JOIN suspension sp
        ON sp.scholar_id=s.scholar_id
LEFT JOIN contact c
        ON s.scholar_id=c.scholar_id
ORDER BY s.scholar_id '''


culture = read_sql(culture_query.format(Date1='2015-11-30', Date2='2016-02-13'), conn_pgsql)

culture['grade'] = culture['grade'].map({0 : 'K', 1 : 1, 2 : 2, 3: 3, 4: 4})


#Pull active scholars for given week
scholars_sms = '''
select
sa.scholarid as scholar_id
from a2_assessment a
inner join a2_scholarassessment sa
        on a.id=sa.assessmentid
inner join scholar s
        on s.id=sa.scholarid
inner join a2_assessmentpart ap
        on ap.assessmentid=a.id
and a.grade < 5
and (ap.GradingDueDate BETWEEN '{Date1}'and '{Date2}')
and s.withdrawalreasonid is null
and a.id not in (3036)
group by sa.scholarid'''


scholar = read_sql(scholars_sms.format(Date1='2016-01-17', Date2='2016-02-13'), conn_mssql)

scholar_culture = pd.merge(scholar, culture, how='left', on='scholar_id')



#PULL Homework and Readidng Log Data from SMS

reading_log_query = '''
SELECT
wk.scholarid as scholar_id,
SUM(CASE
        WHEN goalunits > achievedunits OR achievedunits IS NULL THEN 1
        WHEN goalunits = achievedunits THEN 0
        ELSE 0
END) AS RL_Infractions
FROM scholarweeklygoal wk
WHERE wk.goaltype = 1
AND ((wk.goalyear=2015  AND wk.goalweek in (49,50,51,53) )
OR (wk.goalyear=2016  AND wk.goalweek in (2,3,4,5,6)))
AND achievedunits is not null
GROUP BY wk.scholarid
ORDER BY wk.scholarid'''

reading_log = read_sql(reading_log_query, conn_mssql)


homework_query = '''
SELECT
ScholarID as scholar_id,
COUNT(InfractionType) AS "HW_Infractions"
FROM
(
SELECT
        si.ScholarID,
        CAST(CASE WHEN (si.HomeworkCultureInfraction = 15) THEN 1 ELSE 0 END AS BIT) AS Excused,
        si.HomeworkCultureInfraction AS InfractionType
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
ON ci.ID = si.ClassInfractionsID
WHERE ci.Date between '2015-11-30' and '2016-02-06'
AND si.HomeworkCultureInfraction <> 0
AND ci.IsHomeworkDue = 1
AND (ci.HomeworkDueDateTime < GETDATE() OR si.HomeworkStaffID IS NOT NULL)
) as homework
WHERE Excused='false'
GROUP BY ScholarID
ORDER BY ScholarID;'''


homework = read_sql(homework_query, conn_mssql)


scholar_culture_hw = pd.merge(scholar_culture, homework, how='left', on='scholar_id')

culture_final = pd.merge(scholar_culture_hw, reading_log, how='left', on='scholar_id')

culture_final = culture_final[['scholar_id', 'homeroom', 'school_name', 'school', 'grade','first_name','last_name', 'absent', 'tardy', 'suspensions', 'HW_Infractions', 'RL_Infractions', 'email']]

culture_final = culture_final.fillna(0)

culture_final['email'] = culture_final['email'].replace(0, '')

###PULL ASSESSMENT DATA

#assesment percent
assessment_query_percentage = '''
SELECT
        CASE
                WHEN assessment_type_id=10 THEN points
                WHEN percent IS NULL THEN -1
                ELSE percent
        END as score,
        scholar_id,
        a_name
FROM
        (SELECT
                a.id,
                a.assessment_type_id,
                (ROUND(CAST((sa.points/a.points_possible) AS Numeric), 2)*100) AS percent,
                sa.points,
                sa.scholar_id,
                CASE
                        WHEN assessment_type_id<>11 THEN CONCAT(RIGHT(a.name,-4),' ','percent')
                        WHEN assessment_type_id=11 THEN CONCAT(RIGHT(a.name,-4),' ','letter')
                END as a_name
        FROM scholar_assessment sa
        INNER JOIN assessment a
                ON a.id = sa.assessment_id
        INNER JOIN scholar s
                ON s.id = sa.scholar_id
        WHERE a.grade < 5
        AND s.withdrawal_reason_id IS NULL
        AND assessment_type_id NOT IN (74)
        AND a.id in (2997,2998,3021,3022,3050,3051,3052,3115 ,3116,3117,3118,3119,3120)
        ORDER BY sa.scholar_id) as score  '''

assessment_percentage = read_sql(assessment_query_percentage, conn_pgsql)

assessment_percentage.to_excel('assessment.xls')



assessment_file = pd.ExcelFile('/Users/dwieder/Desktop/ES/assessment.xls')
assessment_file

assessment_file.sheet_names

df = assessment_file.parse('Sheet1')

assessment_percent = pd.pivot_table(df, values='score', index=['scholar_id'], columns=['a_name'])

assessment_percent_updated = assessment_percent.replace(-1, 'Missing')

#only for F&P Assessments
#assessment_percent['F&P Dec15 letter'] = assessment_percent['F&P Dec15 letter'].map({1 : 'A', 2 : 'B', 3 : 'C',  4 : 'D',  5 : 'E', 6 : 'F', 7 : 'G', 8 : 'H', 9 : 'I', 10 : 'J', 11 : 'K', 12 : 'L', 13 : 'M', 14 : 'N', 15 : '0',  16 : 'P',  17 : 'Q',  18 : 'R', 19 : 'S', 20 : 'T', 21 : 'U', 22 : 'V', 23 : 'W', 24 : 'X', 25 : 'Y', 26 : 'Z',})

a_p = assessment_percent_updated.fillna('')

a_p = a_p.reset_index()

#Pull assessments mastery level
assessment_query_mastery= '''
SELECT
        CASE
                WHEN mastery_level IS NULL THEN -1
                ELSE mastery_level
        END AS mastery_level,
        sa.scholar_id,
        CONCAT(RIGHT(a.name,-4),' ', 'mastery') AS a_name
FROM scholar_assessment sa
INNER JOIN assessment a
        ON a.id = sa.assessment_id
INNER JOIN scholar s
        ON s.id = sa.scholar_id
WHERE
a.grade < 5
AND s.exit_date IS NULL
AND assessment_type_id NOT IN (74)
AND a.id in (2997,2998,3021,3022,3050,3051,3052,3115 ,3116,3117,3118,3119,3120)
ORDER BY sa.scholar_id'''

assessment_mastery_level = read_sql(assessment_query_mastery, conn_pgsql)

assessment_mastery_level.to_excel('mastery_level.xls')


mastery_file = pd.ExcelFile('/Users/dwieder/Desktop/ES/mastery_level.xls')
mastery_file

mastery_file.sheet_names

df1 = mastery_file.parse('Sheet1')


assessment_mastery = pd.pivot_table(df1, values='mastery_level', index=['scholar_id'], columns=['a_name'])

assessment_mastery_updated = assessment_mastery.replace(-1, 'Missing')

assessment_mastery_level_final = assessment_mastery_updated.replace([1,2,3,4], ['Below Expectations', 'Approaching Expectations', 'Meeting Expectations', 'Exceeding Expectations' ])

a_m = assessment_mastery_level_final.fillna('')

a_m = a_m.reset_index()


#Merging Assessment Percents and Mastery
assessments = pd.merge(a_p, a_m, how='left', on='scholar_id')

assessments.columns

assessments.columns.name=None

#Re-arrange columns to pair assessments by mastery and percent levels
assessment_final = assessments.iloc[:,[0 , 1 , 12 , 2 , 13, 3 ,14, 4,15, 5,16,6,17,7,18,8,19,9,20,10,21, 11,22]]

assessment_final.to_csv('crazy_time.csv')


#Merge Culture and Academic Data

final = pd.merge(culture_final, assessment_final, how='left', on='scholar_id').set_index('scholar_id')


#Identify scholars who did not have homework marked in SMS
scholars = '''
SELECT
ca.scholar_id,
regexp_replace(s.name, '^Success Academy ', '') AS school_name,
regexp_replace(s.abbreviation, '^SA-', '') AS school,
ga.grade,
sc.nickname,
p.first_name,
p.last_name
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
AND ga.grade in (0,1,2,3,4)
AND ss.withdrawal_reason_id IS NULL
AND ca.end_date IS NULL
AND ga.end_date IS NULL
AND sc.subject_id IS NULL
GROUP BY s.name, ga.grade, sc.nickname, p.first_name, p.last_name, ca.scholar_id,s.abbreviation
ORDER BY ca.scholar_id'''


scholar_hw = read_sql(scholars, conn_pgsql)


homework_not_recorded_query = '''
SELECT
ScholarID as scholar_id,
date as date_homework_not_recorded
FROM
(
SELECT
        si.ScholarID,
        CAST(CASE WHEN (si.HomeworkCultureInfraction = 15) THEN 1 ELSE 0 END AS BIT) AS Excused,
        si.HomeworkCultureInfraction AS InfractionType,
        ci.date
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
ON ci.ID = si.ClassInfractionsID
WHERE ci.Date = '2016-02-12'
AND si.HomeworkCultureInfraction = 16
AND ci.IsHomeworkDue = 1
AND (ci.HomeworkDueDateTime < GETDATE() OR si.HomeworkStaffID IS NOT NULL)
) as homework
WHERE Excused='false'
GROUP BY ScholarID, date
ORDER BY ScholarID, date;
'''


homework_not_recorded = read_sql(homework_not_recorded_query, conn_mssql)


hw_not_recorded = pd.merge(scholar_hw,homework_not_recorded, how='inner', on='scholar_id')


#Create spreadsheet with master, no emails, and distribution of duplicate emails
no_email = final.loc[(final.email.isnull() | (final.email == '') | (final.email == 0))]
tmp = final.loc[~(final.email.isnull() | (final.email == '') | (final.email == 0))]
contacts = tmp.copy()
c = 1
break_flag=False
unique_storage = dict()


excelWriter = pd.ExcelWriter('ES_WPR_2.18.16.xlsx')
final.to_excel(excelWriter,'Master')
no_email.to_excel(excelWriter, 'No Email')
hw_not_recorded.to_excel(excelWriter, 'Homework Not Recorded')

while not break_flag:
    mask = contacts.duplicated(subset=['email','grade'], keep='first')
    unique_emails = contacts.loc[~mask]
    contacts = contacts.loc[mask]
    unique_storage[c] = unique_emails
    if sum(mask) <= 0:
        break_flag=True
    sheet_string = str(c)
    unique_emails.to_excel(excelWriter, sheet_string)

    c += 1
    print 'done'

excelWriter.save()


#
# CRM_percentage_query = '''
# SELECT
#         CASE
#                 WHEN assessment_type_id in (10,52,64) THEN points
#                 WHEN percent IS NULL THEN -1
#                 ELSE percent
#         END as score,
#         scholar_id,
#         Name,
#         title,
#         'percent'
# FROM
#         (SELECT
#                 a.id,
#                 a.assessment_type_id,
#                 (ROUND(CAST((sap.points/ap.points_possible) as numeric),2)*100) as percent,
#                 sa.points,
#                 sa.scholar_id,
#                 RIGHT(a.name,-4) AS Name,
#                 ap.title,
#                 'percent'
#         FROM scholar_assessment sa
#         INNER JOIN assessment a
#                 ON a.id = sa.assessment_id
#         INNER JOIN scholar s
#                 ON s.id = sa.scholar_id
#         INNER JOIN scholar_assessment_part sap
#                 ON sa.id=sap.scholar_assessment_id
#         INNER JOIN assessment_part ap
#                 ON ap.id=sap.assessment_part_id
#         WHERE
#         s.withdrawal_reason_id IS NULL
#         AND assessment_type_id NOT IN (74)
#         AND a.id in (3090,3091)
#         ORDER BY sa.scholar_id) as score
# '''
#
# crm = read_sql(CRM_percentage_query, conn_pgsql)
#
# crm.to_excel('crm_score.xls')
#
#
#
# CRM_mastery_query = '''
# SELECT
#         CASE
#                 WHEN sap.mastery_level IS NULL THEN -1
#                 ELSE sap.mastery_level
#         END AS mastery_level,
#         sa.scholar_id,
#         RIGHT(a.name,-4) AS name,
#         ap.title,
#         'mastery'
# FROM scholar_assessment sa
# LEFT JOIN assessment a
#         ON a.id = sa.assessment_id
# LEFT JOIN scholar s
#         ON s.id = sa.scholar_id
# LEFT JOIN scholar_assessment_part sap
#         ON sa.id=sap.scholar_assessment_id
# LEFT JOIN assessment_part ap
#         ON ap.id=sap.assessment_part_id
# WHERE
# s.withdrawal_reason_id IS NULL
# AND s.exit_date IS NULL
# AND a.id in (3090, 3091)
# ORDER BY sa.scholar_id
# '''
#
# crm_mastery = read_sql(CRM_mastery_query, conn_pgsql)
#
# crm = read_sql(CRM_percentage_query, conn_pgsql)
#
# crm.to_excel('crm_score.xls')