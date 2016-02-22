from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np
import re
from pandas import ExcelWriter
from pandas import ExcelFile

pd.set_option('display.expand_frame_repr',False)

conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')
conn_mysql = create_engine('mysql+pymysql://dna:Harlem.15@192.168.150.159:3306/dna')
conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')


homework = '''
select s.name, COUNT(ca.scholar_id) AS total_scholars, SUM(COALESCE(h.excused IS False, False)::int) AS total_infractions
from class_infractions ci
left join class_assignment ca
   on ca.school_class_id=ci.school_class_id
left join school_class AS sc
   on ca.school_class_id=sc.id
left join school s on s.id=sc.school_id
left join homework h
   on h.scholar_id = ca.scholar_id
   AND ci.date=h.date
where is_homework_due='true'
   and ci.date BETWEEN ca.start_date AND COALESCE(ca.end_date, CURRENT_DATE)
   and ci.date between '2014-08-18' and '2014-10-22'
   and s.school_type=1
GROUP BY s.name
ORDER BY s.name
'''

HW_complete = read_sql(homework, conn_pgsql)
HW_complete.to_csv('hw_complete2.csv')




'2014-08-18' and '2014-10-22'


and ci.date between '2014-08-18' and '2014-10-22'

and ci.date >= '2015-08-17'

(11388, 11389, 11391, 11392, 11395, 11396, 11397, 11400, 11401, 11402, 11403, 11404, 11407, 11408, 11409, 11411, 11414, 11415, 11416, 11419, 11420, 11421, 11422, 11425, 11426, 11427, 11428)



--GROUP BY s.name


homework_submit = '''

select s.name, ca.scholar_id AS total_scholars, COALESCE(h.excused IS False, False), infraction_type
from class_infractions ci
left join class_assignment ca
   on ca.school_class_id=ci.school_class_id
left join school_class AS sc
   on ca.school_class_id=sc.id
left join school s on s.id=sc.school_id
left join homework h
   on h.scholar_id = ca.scholar_id
   AND ci.date=h.date
where is_homework_due='true'
   and ci.date BETWEEN ca.start_date AND COALESCE(ca.end_date, CURRENT_DATE)
   and ci.date >= '2015-08-17'
   and s.school_type=1

'''


x = read_sql(homework_submit, conn_pgsql)




pass_rate_query='''SELECT right(sh.Name,-16) AS School, right(a.name,-4) AS Assessment, a.grade AS Grade,
a.open_date, a.id AS AssessmentID, p.last_name, sc.id AS schoolclassID, subject.name AS Subject,
(SELECT COUNT(*)
FROM scholar_assessment sa1
INNER JOIN school_class sc1 ON sc1.ID=sa1.school_class_id
INNER JOIN assessment a1 ON a1.id=sa1.assessment_id
WHERE sa1.mastery_level IN (3,4)
AND sa1.assessment_id = sa.assessment_id
AND sc.school_id = sc1.school_id
AND sc1.id = sc.id
AND a1.open_date BETWEEN '2015-08-15' AND  '2015-10-20'
and sa1.scholar_id in (select scholar_id from state_exam_results where assessment_type='Math' and proficient_or_advanced='false' and academic_year_id=394)) AS Pass,
(SELECT COUNT(*)
FROM scholar_assessment sa2
INNER JOIN school_class sc2 ON sc2.ID=sa2.school_class_id
INNER JOIN assessment a2 ON a2.id=sa2.assessment_id
INNER JOIN school sh2 ON sh2.ID =sc2.school_id
WHERE sa2.mastery_level IS NOT NULL
AND sa2.assessment_id = sa.assessment_id
AND sc.school_id = sc2.school_id
AND sc2.id = sc.id
AND a2.open_date BETWEEN '2015-08-15' AND  '2015-10-20'
and sa2.scholar_id in (select scholar_id from state_exam_results where assessment_type='Math' and proficient_or_advanced='false' and academic_year_id=394)) AS TotalStudents
FROM scholar_assessment sa
INNER JOIN school_class sc ON sc.ID=sa.school_class_id
INNER JOIN school sh ON sh.ID=sc.school_id
INNER JOIN assessment a ON a.id=sa.assessment_id
INNER JOIN person p ON sc.teacher_staff_id = p.id
INNER JOIN assessment_type ON a.assessment_type_id = assessment_type.id
INNER JOIN subject ON assessment_type.subject_id = subject.id
WHERE a.id=2574
Group BY sc.school_id, sh.name, sa.assessment_id, a.name, a.grade, sa.assessment_date, a.id, sc.id, p.last_name, subject.name
ORDER BY a.open_date, a.name;'''

df5 = pd.read_sql(pass_rate_query, conn_pgsql)
df5.head(30)
​
df6 = (df5.groupby(['open_date', 'assessment','school',  'grade'])['pass'].sum() / df5.groupby(['open_date', 'assessment','school',  'grade'])['totalstudents'].sum()).to_frame('pass_rate_assessment')
df6 = df6.replace([np.inf, -np.inf], np.nan)
df6 = df6.dropna()
df6
0

df7 = (df5.groupby(['open_date', 'assessment', 'grade'])['pass'].sum() / df5.groupby(['open_date', 'assessment',  'grade'])['totalstudents'].sum()).to_frame('pass_rate_assessment')
df7
7
df5.groupby(['school', 'grade'])['totalstudents'].sum()
1
