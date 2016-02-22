from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np

pd.set_option('display.expand_frame_repr',False)

conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')
conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')


scholar_query = '''
select
s.ID as scholarid,
count(assessmentid) as assessments,
DATEDIFF(day,EnrollmentDate, ExitDate) as days_enrolled,
EnrollmentDate,
ExitDate
from scholar s
left join a2_scholarassessment sa
        on s.id=sa.scholarid
inner join a2_assessment a
        on a.id=sa.assessmentid
where withdrawnwithoutattending='True'
and sa.scholarid is not null
and points is not null
and withdrawalreasonid is not null
and withdrawalreasonid <>390
and a.assessmenttypeid not in (85)
group by s.ID, withdrawnwithoutattending,EnrollmentDate,ExitDate
'''

scholar = read_sql(scholar_query, conn_mssql)


absent_query ='''
SELECT
si.scholarid,
count(si.ID) as absent_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
         ON ci.ID = si.ClassInfractionsID
WHERE si.Present = 0
AND si.EarlyDismissal = 0
AND si.LatePickup = 0
AND si.Tardy = 0
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY si.scholarid
'''

absent = read_sql(absent_query, conn_mssql)



present_query = '''
SELECT
si.scholarid,
count(si.id) as present_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
         ON ci.ID = si.ClassInfractionsID
WHERE si.present='true'
AND si.EarlyDismissal = 0
AND si.LatePickup = 0
AND si.Tardy = 0
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY si.scholarid;
'''

present = read_sql(present_query, conn_mssql)

tardy_query = '''
SELECT
si.scholarid,
COUNT(si.ID) as tardy_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
        ON ci.ID = si.ClassInfractionsID
WHERE si.Tardy = 1
AND si.Present = 1
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY scholarid
'''

tardy = read_sql(tardy_query, conn_mssql)


early_dismal_query = '''
SELECT
si.scholarid,
count(scholarid) as early_dismal_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
        ON ci.ID = si.ClassInfractionsID
WHERE si.EarlyDismissal = 1
AND si.Present = 1
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY scholarid
'''

early_dismal = read_sql(early_dismal_query, conn_mssql)

late_pick_up_query = '''
SELECT
si.scholarid,
count(si.ID) as late_pick_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
         ON ci.ID = si.ClassInfractionsID
WHERE si.LatePickup = 1
AND si.Present = 1
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY si.scholarid;
'''

late_pickup = read_sql(late_pick_up_query, conn_mssql)


uniform_query = '''
SELECT
si.scholarid,
count(si.ID) as uniform_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
        ON ci.ID = si.ClassInfractionsID
WHERE si.UniformOK = 0
AND si.Present = 1
AND si.Suspended = 0
AND ci.IsInSession = 1
GROUP BY si.scholarid
'''

uniform = read_sql(uniform_query, conn_mssql)

suspension_query = '''
SELECT scholarid,
COUNT(*) AS suspension_count
FROM suspension
GROUP BY scholarid
'''

suspension  = read_sql(suspension_query, conn_mssql)


homework_infraction_query = '''
SELECT
si.scholarid,
count(si.ID) as homework_infraction_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
        ON ci.ID = si.ClassInfractionsID
WHERE si.HomeworkCultureInfraction <> 0
AND ci.IsHomeworkDue = 1
AND (ci.HomeworkDueDateTime < GETDATE() OR si.HomeworkStaffID IS NOT NULL)
GROUP BY si.ScholarID;
'''

homework_infraction = read_sql(homework_infraction_query, conn_mssql)

homework_completion_query = '''
SELECT
si.scholarid,
count(si.ID) as homework_completion_count
FROM ScholarInfractions AS si
INNER JOIN ClassInfractions AS ci
        ON ci.ID = si.ClassInfractionsID
WHERE si.HomeworkCultureInfraction = 0
AND ci.date in ('2015-08-21', '2015-08-28', '2015-09-04', '2015-09-11',
               '2015-09-18', '2015-09-25', '2015-10-02', '2015-10-09',
               '2015-10-16', '2015-10-23', '2015-10-30', '2015-11-06',
               '2015-11-13', '2015-11-20', '2015-11-27', '2015-12-04',
               '2015-12-11', '2015-12-18', '2015-12-25', '2016-01-01',
               '2016-01-08', '2016-01-15', '2016-01-22', '2016-01-29',
               '2016-02-05')
GROUP BY si.ScholarID;
'''

homework_completion = read_sql(homework_completion_query, conn_mssql)


reading_log_query = '''
SELECT
scholarid,
SUM(achievedunits) AS "Reading Log Minutes"
FROM scholarweeklygoal wk
GROUP BY wk.scholarid;
'''

reading_log = read_sql(reading_log_query, conn_mssql)


df1 = pd.merge(scholar, absent, how='left', on='scholarid')
df2 = pd.merge(df1, present, how='left', on='scholarid')
df3 = pd.merge(df2, tardy, how='left', on='scholarid')
df4 = pd.merge(df3, early_dismal, how='left', on='scholarid')
df5 = pd.merge(df4, late_pickup, how='left', on='scholarid')
df6 = pd.merge(df5, uniform, how='left', on='scholarid')
df7 = pd.merge(df6, suspension, how='left', on='scholarid')
df8 = pd.merge(df7, homework_infraction, how='left', on='scholarid')
df9 = pd.merge(df8, homework_completion, how='left', on='scholarid')
df10 = pd.merge(df9, reading_log, how='left', on='scholarid')

df11 = df10.fillna(0).reset_index('scholarid')

df11.to_csv('missing_scholars2.csv')