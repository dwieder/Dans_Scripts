from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np

pd.set_option('display.expand_frame_repr',False)
conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')
conn_mysql = create_engine('mysql+pymysql://dna:Harlem.15@192.168.150.159:3306/dna')
conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')

scholar_by_subject_query = '''
    DROP AGGREGATE IF EXISTS array_agg_mult (anyarray);
    CREATE AGGREGATE array_agg_mult (anyarray)  (
    SFUNC     = array_cat
   ,STYPE     = anyarray
   ,INITCOND  = '{{}}'
    );

    WITH start_date AS (
    SELECT
        ca.scholar_id,
        MIN(ca.start_date) AS start_date
    FROM class_assignment AS ca
    GROUP BY ca.scholar_id
    ), skip AS (
     SELECT
        scholar_id,
        COUNT(scholar_id)::text AS skip_nav,
        array_agg_mult(ARRAY[ARRAY[academic_year, skip]]) AS skip
     FROM (
        SELECT
            ga.scholar_id,
            ay.description AS academic_year,
            regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS skip
        FROM grade_assignment AS ga
        INNER JOIN academic_year AS ay
            ON ga.academic_year_id = ay.id
        WHERE trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) < COALESCE(grade_shift, 0)
        ORDER BY ay.description
    ) AS skip_data
    GROUP BY scholar_id
), holdover AS (
    SELECT
        scholar_id,
        COUNT(scholar_id)::text AS holdover_nav,
        array_agg_mult(ARRAY[ARRAY[academic_year, holdover]]) AS holdover
    FROM (
        SELECT
            ga.scholar_id,
            ay.description AS academic_year,
            regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS holdover
        FROM grade_assignment AS ga
        INNER JOIN academic_year AS ay
            ON ga.academic_year_id = ay.id
        WHERE trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) > COALESCE(grade_shift, 0)
        ORDER BY ay.description
    ) AS holdover_data
    GROUP BY scholar_id
), ell_status AS (
    SELECT
        ee.scholar_id,
        CASE
            WHEN ee.isell IS True AND ex.isell_grad IS True THEN 'Graduated'
            WHEN ee.isell IS True AND ex.isell_grad IS False THEN 'Not graduated'
            WHEN ee.isell IS True AND ex.isell_grad IS NULL THEN 'Not graduated'
            WHEN ee.isell IS False THEN 'Not an ELL'
        END AS ell_status
    FROM ell_enter AS ee
    LEFT JOIN (
        SELECT
            ex.scholar_id,
            ex.isell_grad,
            ay.description AS academic_year,
            ROW_NUMBER() OVER (PARTITION BY scholar_id ORDER BY ay.start_date DESC) AS most_recent
        FROM ell_exit AS ex
        INNER JOIN academic_year AS ay
            ON ay.id = ex.academic_year_id
    ) AS ex
        ON ee.scholar_id = ex.scholar_id
    WHERE ex.most_recent = 1 OR ex.most_recent IS NULL
), scholars AS (
    SELECT
        sa.scholar_id,
        p.last_name || ', ' || p.first_name AS scholar,
        sd.start_date::text AS start_date,
        regexp_replace(sch.name, '^Success Academy ', '') AS school_name,
        regexp_replace(sch.abbreviation, '^SA-', '') AS school,
        (EXTRACT(year from CURRENT_DATE) - EXTRACT(year FROM sch.start_date))::text AS school_age,
        CASE
            WHEN st.type = 'Elementary' THEN 'ES'
            WHEN st.type = 'Middle' THEN 'MS'
            WHEN st.type = 'High' THEN 'HS'
        END AS school_type,
        ga.grade::int AS grade
    FROM school_assignment AS sa
    INNER JOIN grade_assignment AS ga
        ON sa.scholar_id = ga.scholar_id
    INNER JOIN person AS p
        ON p.id = sa.scholar_id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sa.scholar_id
    INNER JOIN school AS sch
        ON sch.id = sa.school_id
    INNER JOIN school_type AS st
        ON sch.school_type = st.id
    WHERE '{target_date}'::date BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE)
        AND '{target_date}'::date BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE)
        AND NOT EXISTS (
            SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = sa.scholar_id AND w.final_date <= '{target_date}'::date
        )
), sped_raw AS (
    (
    SELECT
        sp.scholar_id,
        spt.type,
        CASE
            WHEN sp.subject_flag = 0 THEN 'General'
            WHEN sp.subject_flag = 1 THEN 'Literacy'
            WHEN sp.subject_flag = 2 THEN 'Math'
            ELSE 'Error'
        END AS subject,
        sp.frequency,
        sp.duration,
        sp.group_size,
        CASE
            WHEN sp.start_date < '2006-06-01' THEN COALESCE(COALESCE(w.final_date, sp.end_date), CURRENT_DATE) - sd.start_date
            WHEN sp.start_date < w.final_date THEN COALESCE(COALESCE(sp.end_date, w.final_date), CURRENT_DATE) - sd.start_date
            ELSE COALESCE(sp.end_date, CURRENT_DATE) - sp.start_date
        END AS time
    FROM sped_program AS sp
    INNER JOIN sped_program_type AS spt
        ON sp.program_type = spt.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    LEFT JOIN withdrawal AS w
        ON w.scholar_id = sp.scholar_id
    WHERE spt.type <> 'General Education'
    )
    UNION ALL
    (
    SELECT DISTINCT
        sp.scholar_id,
        CASE
            WHEN sst.type IN ('Speech Language Therapy', 'Occupational Therapy', 'Counseling' ,'SETSS') THEN sst.type
            ELSE 'Other'
        END AS type,
        'General' AS subject,
        sp.frequency,
        sp.duration,
        sp.group_size,
        CASE
            WHEN sp.start_date < '2006-06-01' THEN COALESCE(COALESCE(w.final_date, sp.end_date), CURRENT_DATE) - sd.start_date
            WHEN sp.start_date < w.final_date THEN COALESCE(COALESCE(sp.end_date, w.final_date), CURRENT_DATE) - sd.start_date
            ELSE COALESCE(sp.end_date, CURRENT_DATE) - sp.start_date
        END AS time
    FROM sped_service AS sp
    INNER JOIN sped_service_type AS sst
        ON sp.service_type = sst.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    LEFT JOIN withdrawal AS w
        ON w.scholar_id = sp.scholar_id
    WHERE sst.type NOT LIKE 'At Risk%%'
    )
), all_sped AS (
    SELECT
        p.scholar_id,
        p.type,
        p.subject,
        p.frequency::text,
        p.duration::text,
        p.group_size::text,
        round(p.time / 365.0, 1)::text AS time
    FROM sped_raw AS p
    WHERE p.time > 0 AND p.frequency > 0 AND p.duration > 0
), sped AS (
    SELECT
        asp.scholar_id,
        array_agg(DISTINCT asp.type) AS sped_nav,
        array_agg_mult(ARRAY[ARRAY[
            asp.type, asp.subject, asp.frequency::text, asp.duration::text, asp.group_size::text, asp.time::text
        ]]) AS sped
    FROM all_sped AS asp
    GROUP BY asp.scholar_id
),  ser AS (
    SELECT
        ser.scholar_id,
        ay.description AS academic_year,
        CASE
            WHEN ser.assessment_type = 'ELA' THEN 'Literacy'
            WHEN ser.assessment_type = 'Math' THEN 'Mathematics'
            ELSE 'Science'
        END AS subject,
        ser.level_achieved AS exam_score
    FROM state_exam_results AS ser
    INNER JOIN academic_year AS ay
        ON ser.academic_year_id = ay.id
    ORDER BY ay.description
), state_exam AS (
    SELECT
        scholar_id,
        subject,
        array_agg(DISTINCT exam_score::text) AS state_exam_nav,
        array_agg_mult(ARRAY[ARRAY[subject, academic_year, exam_score::text]]) AS state_exam
    FROM ser
    GROUP BY scholar_id, subject
), grade_subjects AS (
    SELECT DISTINCT
        a.grade,
        at.subject_id,
        s.name AS subject
    FROM assessment AS a
    INNER JOIN assessment_type AS at
        ON at.id = a.assessment_type_id
    INNER JOIN subject AS s
        ON s.id = at.subject_id
    WHERE s.name IN ('Literacy', 'Mathematics', 'Science', 'History')
), teachers_prep AS (
    SELECT
        ca.scholar_id,
        gs.subject,
        ga.grade,
        p.last_name || ', ' || p.first_name AS teacher,
        ROW_NUMBER() OVER
            (PARTITION BY ca.scholar_id, gs.subject ORDER BY ca.scholar_id, COALESCE(ca.subject_id, -10)) AS rnum_es,
        ROW_NUMBER() OVER (PARTITION BY ca.scholar_id, gs.subject ORDER BY ca.scholar_id, ca.subject_id) AS rnum_mshs
    FROM class_assignment AS ca
    INNER JOIN grade_assignment AS ga
        ON ga.scholar_id = ca.scholar_id
    INNER JOIN school_class AS sc
        ON sc.id = ca.school_class_id
    INNER JOIN person AS p
        ON p.id = sc.teacher_staff_id
    LEFT JOIN grade_subjects AS gs
        ON ga.grade = gs.grade
        AND (ca.subject_id = gs.subject_id OR ca.subject_id IS NULL)
    WHERE '{target_date}'::date BETWEEN ca.start_date AND COALESCE(ca.end_date, CURRENT_DATE)
        AND '{target_date}'::date BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE)
        AND gs.subject IS NOT NULL
), teacher AS (
    SELECT
        scholar_id,
        subject,
        teacher
    FROM teachers_prep
    WHERE ((rnum_es = 1 AND grade IN (-1, 0, 1, 2, 3, 4))
        OR (rnum_mshs = 1 AND grade NOT IN (-1, 0, 1, 2, 3, 4)))
)

SELECT
    s.scholar_id,
    s.scholar,
    s.start_date,
    s.school_name,
    s.school,
    s.school_age,
    s.school_type,
    s.grade,
    t.subject,
    COALESCE(ell.ell_status, 'Not evaluated') AS ell_status,
    COALESCE(skip.skip, string_to_array('', '')) AS skip,
    COALESCE(skip.skip_nav, '0') AS skip_nav,
    COALESCE(hold.holdover, string_to_array('', '')) AS holdover,
    COALESCE(hold.holdover_nav, '0') AS holdover_nav,
    COALESCE(sped.sped, string_to_array('', '')) AS sped,
    COALESCE(sped.sped_nav, ARRAY['No services']) AS sped_nav,
    COALESCE(se.state_exam, string_to_array('', '')) AS state_exam,
    COALESCE(se.state_exam_nav, ARRAY['None']) AS state_exam_nav,
    COALESCE(t.teacher, 'Unknown') AS teacher
FROM scholars AS s
LEFT JOIN teacher AS t
    ON s.scholar_id = t.scholar_id
LEFT JOIN state_exam AS se
    ON se.subject = t.subject
    AND se.scholar_id = t.scholar_id
LEFT JOIN skip
    ON skip.scholar_id = s.scholar_id
LEFT JOIN holdover AS hold
    ON hold.scholar_id = s.scholar_id
LEFT JOIN ell_status AS ell
    ON ell.scholar_id = s.scholar_id
LEFT JOIN sped
    ON sped.scholar_id = s.scholar_id
WHERE NOT EXISTS (
            SELECT 1 FROM withdrawal AS w
            WHERE w.scholar_id = s.scholar_id AND w.final_date <= '{target_date}'::date
        )
'''


scholar_achievement_query = '''
WITH decayed_scores AS (
    SELECT
        qqs.subject_id,
        qqs.assessment_id,
        qqs.assessment_question_id,
        exp(1.0)^((ln(0.5) / 28) * ('{target_date}'::date - a.due_date)) * qqs.score::float AS weight
    FROM question_quality_scores AS qqs
    INNER JOIN assessment AS a
        ON qqs.assessment_id = a.id
    INNER JOIN assessment_type AS at
        ON a.assessment_type_id = at.id
    WHERE a.due_date <= '{target_date}'::date
        AND a.due_date >= ('{target_date}'::date - 180)
        AND at.description NOT IN('Incoming Scholar Assessment', 'Math Olympiad Contest')
), academic_achievement_snapshot AS (
    SELECT
        ds.subject_id,
        saap.scholar_id,
        '{target_date}'::date AS reference_date,
        SUM (ds.weight * saap.percent_correct) / SUM (ds.weight) AS score
    FROM scholar_assessment_answer_percent AS saap
    INNER JOIN decayed_scores AS ds
        ON ds.assessment_id = saap.assessment_id
        AND ds.assessment_question_id = saap.assessment_question_id
    WHERE NOT EXISTS (
        SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = saap.scholar_id AND w.final_date <= '{target_date}'::date
    )
    GROUP BY ds.subject_id, saap.scholar_id
)
SELECT
    saas.scholar_id,
    /*saas.reference_date,*/
    s.name AS subject,
    ga.grade::int,
    saas.score AS achievement,
    ga.academic_year_id
FROM academic_achievement_snapshot AS saas
INNER JOIN grade_assignment AS ga
    ON ga.scholar_id = saas.scholar_id
INNER JOIN (SELECT id, name FROM subject WHERE name IN ('Literacy', 'Mathematics', 'Science', 'History')) AS s
    ON s.id = saas.subject_id
WHERE saas.reference_date = '{target_date}'::date
    AND '{target_date}'::date >= ga.start_date
    AND '{target_date}'::date <= COALESCE(ga.end_date, '{target_date}'::date)
'''



#Academic Data for AY 2015-2016

scholar_subject_16 = read_sql(scholar_by_subject_query.format(target_date='2015-10-15'), conn_pgsql)#.set_index(['scholar_id', 'subject', 'grade'])
scholar_achieve_16 = read_sql(scholar_achievement_query.format(target_date='2015-10-15'), conn_pgsql)#.set_index(['scholar_id','subject', 'grade'])
# scholar = scholar_subject.join(scholar_achieve, on = ['scholar_id','subject','grade'], how='inner').reset_index()

scholar_achieve_16

scholar_16 = merge(scholar_subject_16,scholar_achieve_16, on = ['scholar_id','subject', 'grade'])
scholar_16 = scholar_16[['scholar_id','achievement', 'academic_year_id']]
scholar_16.head()



Academics_2016 = scholar_16.loc[scholar_16.school.eq('H2L')]
Academics_2016
round(Academics_2016['achievement'].median(), 2)
& scholar_16.subject.eq('Science')








#Academic Data for AY 2014-2015

scholar_subject_15 = read_sql(scholar_by_subject_query.format(target_date='2015-06-22'), conn_pgsql)#.set_index(['scholar_id', 'subject', 'grade'])
scholar_achieve_15 = read_sql(scholar_achievement_query.format(target_date='2015-06-22'), conn_pgsql)#.set_index(['scholar_id','subject', 'grade'])
# scholar = scholar_subject.join(scholar_achieve, on = ['scholar_id','subject','grade'], how='inner').reset_index()


scholar_15 = merge(scholar_subject_15,scholar_achieve_15, on = ['scholar_id','subject', 'grade'])
scholar_15 = scholar_15[['scholar_id','scholar','school_name','school', 'grade', 'subject', 'teacher','sped', 'sped_nav', 'achievement']]


# Academics AY 2015-2016

Academics_2015 = scholar_15.loc[scholar_15.school.eq('HNC') & scholar_16.grade.eq(4)]
Academics_2015
round(Academics_2015['achievement'].median(), 2)

Academics_15 = scholar_15.loc[scholar_15.school.eq('WB')]# & scholar_15.teacher.str.contains('Lin')]
Academics_15
round(Academics_15['achievement'].median(), 2)


Academics_15 = scholar_15.loc[scholar_15.school.eq('HW') & scholar_15.teacher.eq('Simmons, Nicholas') & scholar_15.subject.eq('Mathematics')]
Academics_15 = scholar_15.loc[scholar_15.school.eq('WB') & scholar_15.grade.eq(3) & scholar_15.teacher.eq('Stapleton, Cinnamon')]
Academics_15
round(Academics_15['achievement'].median(), 2)




Academics_2016 = scholar_16.loc[scholar_16.school.eq('HC')]#  & scholar_16.subject.eq('Science') & scholar_16.teacher.str.contains('Hanson')]
Academics_2016
round(Academics_2016['achievement'].median(), 2)

 #& scholar_16.scholar_id.isin(bx4_ICT)
 #& scholar_16.grade.eq(2)
Academics_2016 = scholar_16.loc[scholar_16.school.eq('BX2')  & scholar_16.scholar_id.isin([11926,	30560,	30631,	32254,	32372,	37859,	2300030317,	2300030863,	2300031527,	2300033115,	2300033993,	2300034946,	2300035162,	2300107269,	2300157482])]
Academics_2016
round(Academics_2016['achievement'].median(), 2)
Academics_2016.to_csv('sped6.csv')


wb_SETSS = [6960,	9542,	10083,	11467,	14002,	15180,	15758,	17586,	24165,	24665,	32742,	33956,	38834,	43922,	45753,	48468,	49845,	2300030653,	2300031550,	2300031648,	2300032560,	2300032918,	2300034284,	2300096392,	2300103094,	2300105930]
bx4_ICT = [2300031157 ,2300034506 ,2300035274 ,2300036314]
cr_ict = [39477,	2300030627,	2300032998,	2300033090,	2300035570,	2300035866,	2300036398,	2300101544]
bs2_ict = [13597 ,25268 ,26065 ,2300036837, 2300153943]


schools = np.unique(scholar_culture_016.school)
results_df = pd.DataFrame(columns=['school','grade','achievement'])
for sch in schools:
    sch_df = scholar_culture_016.loc[scholar_culture_016.school == sch]
    grades = np.unique(sch_df.grade)
    for g in grades:
        #g_df = sch_df.loc[sch_df.grade == g]
        g_df = sch_df.loc[sch_df.grade == g & sch_df.scholar_id.isin([2300031157 ,2300034506 ,2300035274 ,2300036314])]
        val = np.median(g_df.achievement)
        tmp = pd.DataFrame(data={'school': sch, 'grade': g, 'achievement': val}, index=[0])
        results_df = results_df.append(tmp, ignore_index=True)

results_df
results_df.loc[results_df.school.eq('H1')]# & results_df.grade.eq(5)])
d = results_df.loc[results_df.school.eq('HC')]   # & results_df.grade.eq
d
np.median(d.achievement)


#2015-2016
schools = np.unique(scholar_culture_016.school)
results_df = pd.DataFrame(columns=['school','grade','teacher', 'achievement'])
for sch in schools:
    sch_df = scholar_culture_016.loc[scholar_culture_016.school == sch]
    grades = np.unique(sch_df.grade)
    for g in grades:
        g_df = sch_df.loc[sch_df.grade == g]
        #g_df = sch_df.loc[sch_df.grade == g & sch_df.scholar_id.isin(bx4_ICT.values)]
        val = np.median(g_df.achievement)
        teachers = np.unique(sch_df.teacher)
        for t in teachers:
            t_df = sch_df.loc[sch_df.teacher == t]
            val = np.median(t_df.achievement)
            tmp = pd.DataFrame(data={'school': sch, 'grade': g, 'teacher': t, 'achievement': val}, index=[0])
            results_df = results_df.append(tmp, ignore_index=True)


results_df
results_df.loc[results_df.school.eq('UW') & results_df.grade.eq(4)]
d = results_df.loc[results_df.school.eq('HNW')]1
np.median(d.achievement)


#2014-2015
schools = np.unique(scholar_culture_015.school)
results_df = pd.DataFrame(columns=['school','grade','achievement'])
for sch in schools:
    sch_df = scholar_culture_015.loc[scholar_culture_015.school == sch]
    grades = np.unique(sch_df.grade)
    for g in grades:
        g_df = sch_df.loc[sch_df.grade == g]
        #g_df = sch_df.loc[sch_df.grade == g & sch_df.scholar_id.isin(JM_HE_15_SETTS.values)]
        val = np.median(g_df.achievement)
        tmp = pd.DataFrame(data={'school': sch, 'grade': g, 'achievement': val}, index=[0])
        results_df = results_df.append(tmp, ignore_index=True)


results_df
np.median(results_df.loc[results_df.school.eq('CH')])

np.median(results_df.achievement.loc[results_df.school.eq('H2U')])






len(results_df)


round(np.median(results_df.achievement.loc[results_df.school == 'BSMS']), 2)

schools = np.unique(scholar_culture_015.school)
results_df1 = pd.DataFrame(columns=['school','grade','teacher', 'achievement'])
for sch in schools:
    sch_df = scholar_culture_015.loc[scholar_culture_015.school == sch]
    grades = np.unique(sch_df.grade)
    for g in grades:
        g_df = sch_df.loc[sch_df.grade == g]
        #g_df = sch_df.loc[sch_df.grade == g & sch_df.scholar_id.isin(AF_CH_15_SETSS.values)
        val = np.median(g_df.achievement)
        teachers = np.unique(sch_df.teacher)
        for t in teachers:
            t_df = sch_df.loc[sch_df.teacher == t]
            val = np.median(t_df.achievement)
            tmp = pd.DataFrame(data={'school': sch, 'grade': g, 'teacher': t, 'achievement': val}, index=[0])
            results_df1 = results_df1.append(tmp, ignore_index=True)


results_df1.loc[results_df1.school.eq('BX4') & results_df1.grade.eq(1)]

results = results_df1.loc[results_df1.school.eq('UW') & results_df1.grade.eq(5)]
np.median(results.achievement)

results_df1.loc[results_df1.school == 'CR']

np.median([3.2])

round(np.median(results_df1.achievement.loc[results_df1.school == 'BS1']), 2)

results_df1.loc[results_df1.school == 'BS2' & results_df1.grade.isin([0, 1, 2])]


scholar_15.loc[scholar_15.teacher.str.contains('Yewdell')]

###CULTURE

scholar_by_culture_query = '''
DROP TABLE IF EXISTS metric_summary;
CREATE TEMPORARY TABLE metric_summary (
    scholar_id bigint,
    subject varchar(200),
    achievement double precision
);
DROP TABLE IF EXISTS reading_log;
CREATE TEMPORARY TABLE reading_log AS (
    SELECT
        swg.scholar_id,
        (to_timestamp('1 ' || swg.goal_year, 'IW IYYY')::DATE + (7 * swg.goal_week)::int - 7)::date AS date
    FROM scholar_weekly_goal AS swg
    INNER JOIN goal_type AS gt
        ON swg.goal_type = gt.id
    WHERE gt.type = 'Reading'
        AND COALESCE(swg.achieved_units, 0.0) < swg.goal_units
);
DROP TABLE IF EXISTS target_days;
CREATE TEMPORARY TABLE target_days AS (
    SELECT
        ci.date,
        ci.school_class_id,
        exp(1.0)^((ln(0.5) / 45) * ('{target_date}'::date - ci.date)) *
            ci.is_in_session::int::float AS is_in_session,
        exp(1.0)^((ln(0.5) / 45) * ('{target_date}'::date - ci.date)) *
            ci.is_homework_due::int::float AS is_homework_due,
        exp(1.0)^((ln(0.5) / 45) * ('{target_date}'::date - ci.date)) *
            (rl.date IS NOT NULL)::int::float AS is_log_due
    FROM class_infractions AS ci
    LEFT JOIN (SELECT DISTINCT date FROM reading_log) AS rl
        ON rl.date = ci.date
    WHERE ci.date <= '{target_date}'::date
        AND ci.date >= ('{target_date}'::date - 180)
);
DROP TABLE IF EXISTS all_scholars;
CREATE TEMPORARY TABLE all_scholars AS (
    SELECT
        ca.scholar_id,
        td.date,
        td.is_in_session,
        td.is_homework_due,
        td.is_log_due
    FROM target_days AS td
    INNER JOIN class_assignment AS ca
        ON ca.school_class_id = td.school_class_id
        AND td.date >= ca.start_date
        AND td.date <= COALESCE(ca.end_date, CURRENT_DATE)
        AND NOT EXISTS (SELECT 1 FROM withdrawal AS w WHERE ca.scholar_id = w.scholar_id AND w.final_date < td.date)
);
INSERT INTO metric_summary (scholar_id,subject,achievement)
(
SELECT
    asch.scholar_id,
    'absence' AS subject,
    SUM((a1.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN absence AS a1
    ON a1.scholar_id = asch.scholar_id
    AND a1.date = asch.date
    AND a1.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'absence_call' AS subject,
    SUM((a2.call IS False)::int::float * asch.is_in_session) /
        NULLIF(SUM((a2.scholar_id IS NOT NULL)::int::float * asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN absence AS a2
    ON a2.scholar_id = asch.scholar_id
    AND a2.date = asch.date
    AND a2.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'tardy' AS subject,
    SUM((t.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN tardy AS t
    ON t.scholar_id = asch.scholar_id
    AND t.date = asch.date
    AND t.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'early_dismissal' AS subject,
    SUM((e.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN early_dismissal AS e
    ON e.scholar_id = asch.scholar_id
    AND e.date = asch.date
    AND e.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'late_pickup' AS subject,
    SUM((l.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN late_pickup AS l
    ON l.scholar_id = asch.scholar_id
    AND l.date = asch.date
    AND l.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'uniform' AS subject,
    SUM((u.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN uniform AS u
    ON u.scholar_id = asch.scholar_id
    AND u.date = asch.date
    AND u.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'homework' AS subject,
    SUM((h.scholar_id IS NOT NULL)::int::float * asch.is_homework_due) / NULLIF(SUM(asch.is_homework_due), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN homework AS h
    ON h.scholar_id = asch.scholar_id
    AND h.date = asch.date
    AND h.excused = False
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'suspended' AS subject,
    SUM((s.scholar_id IS NOT NULL)::int::float * asch.is_in_session) / NULLIF(SUM(asch.is_in_session), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN suspension AS s
    ON s.scholar_id = asch.scholar_id
    AND asch.date >= s.start_date
    AND asch.date <= s.end_date
GROUP BY asch.scholar_id
)
UNION ALL
(
SELECT
    asch.scholar_id,
    'reading_log' AS subject,
    SUM((r.scholar_id IS NOT NULL)::int::float * asch.is_log_due) / NULLIF(SUM(asch.is_log_due), 0) AS weight
FROM all_scholars AS asch
LEFT JOIN reading_log AS r
    ON r.scholar_id = asch.scholar_id
    AND r.date = asch.date
GROUP BY asch.scholar_id
);
SELECT
    ms.scholar_id,
    ga.grade,
    ms.subject,
    1 - ms.achievement AS achievement,
    ga.academic_year_id
FROM grade_assignment AS ga
INNER JOIN metric_summary AS ms
    ON ga.scholar_id = ms.scholar_id
WHERE '{target_date}'::date >= ga.start_date
    AND '{target_date}'::date <= COALESCE(ga.end_date, CURRENT_DATE)
    AND NOT (ms.subject = 'homework' AND ga.grade IN (5, 6, 7, 8, 9, 10, 11, 12))
'''




scholar_query = '''
DROP AGGREGATE IF EXISTS array_agg_mult (anyarray);
CREATE AGGREGATE array_agg_mult (anyarray)  (
    SFUNC     = array_cat
   ,STYPE     = anyarray
   ,INITCOND  = '{{}}'
);

WITH start_date AS (
    SELECT
        ca.scholar_id,
        MIN(ca.start_date) AS start_date
    FROM class_assignment AS ca
    GROUP BY ca.scholar_id
), skip AS (
    SELECT
        scholar_id,
        COUNT(scholar_id)::text AS skip_nav,
        array_agg_mult(ARRAY[ARRAY[academic_year, skip]]) AS skip
    FROM (
        SELECT
            ga.scholar_id,
            ay.description AS academic_year,
            regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS skip
        FROM grade_assignment AS ga
        INNER JOIN academic_year AS ay
            ON ga.academic_year_id = ay.id
        WHERE trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) < COALESCE(grade_shift, 0)
        ORDER BY ay.description
    ) AS skip_data
    GROUP BY scholar_id
), holdover AS (
    SELECT
        scholar_id,
        COUNT(scholar_id)::text AS holdover_nav,
        array_agg_mult(ARRAY[ARRAY[academic_year, holdover]]) AS holdover
    FROM (
        SELECT
            ga.scholar_id,
            ay.description AS academic_year,
            regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS holdover
        FROM grade_assignment AS ga
        INNER JOIN academic_year AS ay
            ON ga.academic_year_id = ay.id
        WHERE trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) > COALESCE(grade_shift, 0)
        ORDER BY ay.description
    ) AS holdover_data
    GROUP BY scholar_id
), ell_status AS (
    SELECT
        ee.scholar_id,
        CASE
            WHEN ee.isell IS True AND ex.isell_grad IS True THEN 'Graduated'
            WHEN ee.isell IS True AND ex.isell_grad IS False THEN 'Not graduated'
            WHEN ee.isell IS True AND ex.isell_grad IS NULL THEN 'Not graduated'
            WHEN ee.isell IS False THEN 'Not an ELL'
        END AS ell_status
    FROM ell_enter AS ee
    LEFT JOIN (
        SELECT
            ex.scholar_id,
            ex.isell_grad,
            ay.description AS academic_year,
            ROW_NUMBER() OVER (PARTITION BY scholar_id ORDER BY ay.start_date DESC) AS most_recent
        FROM ell_exit AS ex
        INNER JOIN academic_year AS ay
            ON ay.id = ex.academic_year_id
     ) AS ex
        ON ee.scholar_id = ex.scholar_id
    WHERE ex.most_recent = 1 OR ex.most_recent IS NULL
), scholars AS (
    SELECT
        sa.scholar_id,
        p.last_name || ', ' || p.first_name AS scholar,
        sd.start_date::text AS start_date,
        regexp_replace(sch.name, '^Success Academy ', '') AS school_name,
        regexp_replace(sch.abbreviation, '^SA-', '') AS school,
        /*(EXTRACT(year from CURRENT_DATE) - EXTRACT(year FROM sch.start_date))::text AS school_age,*/
        ROUND(CURRENT_DATE - sch.start_date)::text AS school_age,
        CASE
            WHEN st.type = 'Elementary' THEN 'ES'
            WHEN st.type = 'Middle' THEN 'MS'
            WHEN st.type = 'High' THEN 'HS'
        END AS school_type,
        ga.grade::int AS grade
    FROM school_assignment AS sa
    INNER JOIN grade_assignment AS ga
        ON sa.scholar_id = ga.scholar_id
    INNER JOIN person AS p
        ON p.id = sa.scholar_id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sa.scholar_id
    INNER JOIN school AS sch
        ON sch.id = sa.school_id
    INNER JOIN school_type AS st
        ON sch.school_type = st.id
    WHERE '{target_date}'::date BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE)
        AND '{target_date}'::date BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE)
        AND NOT EXISTS (
            SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = sa.scholar_id AND w.final_date <= '{target_date}'::date
        )
), sped_raw AS (
    (
    SELECT
        sp.scholar_id,
        spt.type,
        CASE
            WHEN sp.subject_flag = 0 THEN 'General'
            WHEN sp.subject_flag = 1 THEN 'Literacy'
            WHEN sp.subject_flag = 2 THEN 'Math'
            ELSE 'Error'
        END AS subject,
        sp.frequency,
        sp.duration,
        sp.group_size,
        CASE
            WHEN sp.start_date < '2006-06-01' THEN COALESCE(COALESCE(w.final_date, sp.end_date), CURRENT_DATE) - sd.start_date
            WHEN sp.start_date < w.final_date THEN COALESCE(COALESCE(sp.end_date, w.final_date), CURRENT_DATE) - sd.start_date
            ELSE COALESCE(sp.end_date, CURRENT_DATE) - sp.start_date
        END AS time
    FROM sped_program AS sp
    INNER JOIN sped_program_type AS spt
        ON sp.program_type = spt.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    LEFT JOIN withdrawal AS w
        ON w.scholar_id = sp.scholar_id
    WHERE spt.type <> 'General Education'
    )
    UNION ALL
    (
    SELECT DISTINCT
        sp.scholar_id,
        CASE
            WHEN sst.type IN ('Speech Language Therapy', 'Occupational Therapy', 'Counseling' ,'SETSS') THEN sst.type
            ELSE 'Other'
        END AS type,
        'General' AS subject,
        sp.frequency,
        sp.duration,
        sp.group_size,
        CASE
            WHEN sp.start_date < '2006-06-01' THEN COALESCE(COALESCE(w.final_date, sp.end_date), CURRENT_DATE) - sd.start_date
            WHEN sp.start_date < w.final_date THEN COALESCE(COALESCE(sp.end_date, w.final_date), CURRENT_DATE) - sd.start_date
            ELSE COALESCE(sp.end_date, CURRENT_DATE) - sp.start_date
        END AS time
    FROM sped_service AS sp
    INNER JOIN sped_service_type AS sst
        ON sp.service_type = sst.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    LEFT JOIN withdrawal AS w
        ON w.scholar_id = sp.scholar_id
    WHERE sst.type NOT LIKE 'At Risk%%'
    )
), all_sped AS (
    SELECT
        p.scholar_id,
        p.type,
        p.subject,
        p.frequency::text,
        p.duration::text,
        p.group_size::text,
        round(p.time / 365.0, 1)::text AS time
    FROM sped_raw AS p
    WHERE p.time > 0 AND p.frequency > 0 AND p.duration > 0
), sped AS (
    SELECT
        asp.scholar_id,
        array_agg(DISTINCT asp.type) AS sped_nav,
        array_agg_mult(ARRAY[ARRAY[
            asp.type, asp.subject, asp.frequency::text, asp.duration::text, asp.group_size::text, asp.time::text
        ]]) AS sped
    FROM all_sped AS asp
    GROUP BY asp.scholar_id
)

SELECT
    s.scholar_id,
    s.scholar,
    s.start_date,
    s.school_name,
    s.school,
    s.school_age,
    s.school_type,
    s.grade :: int,
    p.last_name || ', ' || p.first_name AS teacher,
    COALESCE(ell.ell_status, 'Not evaluated') AS ell_status,
    COALESCE(skip.skip, string_to_array('', '')) AS skip,
    COALESCE(skip.skip_nav, '0') AS skip_nav,
    COALESCE(hold.holdover, string_to_array('', '')) AS holdover,
    COALESCE(hold.holdover_nav, '0') AS holdover_nav,
    COALESCE(sped.sped, string_to_array('', '')) AS sped,
    COALESCE(sped.sped_nav, ARRAY['No services']) AS sped_nav
FROM scholars AS s
LEFT JOIN skip
    ON skip.scholar_id = s.scholar_id
LEFT JOIN holdover AS hold
    ON hold.scholar_id = s.scholar_id
LEFT JOIN ell_status AS ell
    ON ell.scholar_id = s.scholar_id
LEFT JOIN sped
    On sped.scholar_id = s.scholar_id
LEFT JOIN class_assignment AS ca
    ON ca.scholar_id = s.scholar_id
LEFT JOIN school_class AS sc
    ON sc.id = ca.school_class_id
LEFT JOIN person as p
    ON p.id = sc.teacher_staff_id
WHERE NOT EXISTS (
            SELECT 1 FROM withdrawal AS w
            WHERE w.scholar_id = s.scholar_id AND w.final_date <= '{target_date}'::date
        )
    AND '{target_date}'::date >= ca.start_date
    AND '{target_date}'::date <= COALESCE(ca.end_date, CURRENT_DATE)
    AND ca.allow_multiple_grades IS False
    AND ca.subject_id IS NULL
'''


scholar_culture_16 = read_sql(scholar_by_culture_query, conn_pgsql)
scholar_info_16= read_sql(scholar_query, conn_pgsql)




scholar_cult_16 = merge(scholar_culture_16,scholar_info_16, on = ['scholar_id','grade'])
scholar_culture_016 = scholar_cult_16[['scholar_id','scholar','school_name','school', 'grade', 'subject','teacher', 'achievement']]





# Culture Data for AY 15-16
scholar_culture_16 = read_sql(scholar_by_culture_query.format(target_date='2015-10-15'), conn_pgsql)
scholar_info_16= read_sql(scholar_query.format(target_date='2015-10-15'), conn_pgsql)




scholar_cult_16 = merge(scholar_culture_16,scholar_info_16, on = ['scholar_id','grade'])
scholar_culture_016 = scholar_cult_16[['scholar_id','scholar','school_name','school', 'grade', 'subject','teacher', 'achievement']]


scholar_culture_016 = scholar_culture_016.loc[scholar_culture_016.school.eq('H2L')]
scholar_culture_016
np.median(scholar_culture_016.achievement)




scholar_culture_15 = read_sql(scholar_by_culture_query.format(target_date='2015-06-26'), conn_pgsql)
scholar_info_15= read_sql(scholar_query.format(target_date='2015-06-26'), conn_pgsql)


scholar_cult_15 = merge(scholar_culture_15,scholar_info_15, on = ['scholar_id','grade'])
scholar_culture_015 = scholar_cult_15[['scholar_id','scholar','school_name','school', 'grade', 'subject', 'teacher', 'achievement']]




scholar =  scholar_culture_015.loc[scholar_culture_015.school.eq('HC')]# & scholar_culture_016.scholar_id.isin([6960,	9542,	10083,	11467,	14002,	15180,	15758,	17586,	24165,	24665,	32742,	33956,	38834,	43922,	45753,	48468,	49845,	2300030653,	2300031550,	2300031648,	2300032560,	2300032918,	2300034284,	2300096392,	2300103094,	2300105930])]
scholar
np.median(scholar.achievement)


Academics_2016['scholar_id'].to_csv('scholar.csv')


wb_SETSS = [6960,	9542,	10083,	11467,	14002,	15180,	15758,	17586,	24165,	24665,	32742,	33956,	38834,	43922,	45753,	48468,	49845,	2300030653,	2300031550,	2300031648,	2300032560,	2300032918,	2300034284,	2300096392,	2300103094,	2300105930]
bx4_ICT = [2300031157 ,2300034506 ,2300035274 ,2300036314]
cr_ict = [39477,	2300030627,	2300032998,	2300033090,	2300035570,	2300035866,	2300036398,	2300101544]
bs2_ict = [13597 ,25268 ,26065 ,2300036837, 2300153943]

# Culture Data for AY 14-15

scholar_culture_15 = read_sql(scholar_by_culture_query.format(target_date='2015-06-26'), conn_pgsql)
scholar_info_15= read_sql(scholar_query.format(target_date='2015-06-26'), conn_pgsql)


scholar_cult_15 = merge(scholar_culture_15,scholar_info_15, on = ['scholar_id','grade'])
scholar_culture_015 = scholar_cult_15[['scholar_id','scholar','school_name','school', 'grade', 'subject', 'teacher', 'achievement']]
scholar_culture_015.head()

np.median(results_df1.achievement.loc[results_df1.school == 'CH'])

teacher = scholar_culture_015.loc[scholar_culture_015.school.eq('HNC') & scholar_culture_015.grade.eq(4)]
teacher
np.median(teacher.achievement)


teacher = scholar_culture_015.loc[scholar_culture_015.scholar_id.isin([8408,	10388,	11396,	17518,	19856,	20788,	22005,	27920,	28942,	31660,	40048,	45800,	46477,	2300030701,	2300031072,	2300031565,	2300034732]) & scholar_culture_015.school.eq('PH')]
teacher
np.median(teacher.achievement)
teacher.achievement.to_csv('score.csv')

teacher.to_csv('teacher.csv')


schools = np.unique(df.school)
for sch in schools:
    sch_df = df.loc[df.school == sch]
    LRs = np.unique(sch_df.candidate_name)
    for lr in LRs:
        lr_df = sch_df.loc[sch_df.candidate_name == lr]
        math here

Academics_2015 = scholar_15.loc[scholar_15.school.eq('PH') & scholar_15.scholar_id.isin([8408,	10388,	11396,	17518,	19856,	20788,	22005,	27920,	28942,	31660,	40048,	45800,	46477,	2300030701,	2300031072,	2300031565,	2300034732])]
Academics_2015
round(Academics_2015['achievement'].median(), 2)
Academics_2015.to_csv('sped6.csv')
