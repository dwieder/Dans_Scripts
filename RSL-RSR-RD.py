import pandas as pd
import sqlalchemy
import numpy as np
import re


pd.set_option('display.expand_frame_repr',False)

conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')



#scholar_by_subject_query
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

#scholar_achievement_query
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
    /*s.name AS subject,*/
    ga.grade::int,
    saas.score AS achievement
FROM academic_achievement_snapshot AS saas
INNER JOIN grade_assignment AS ga
    ON ga.scholar_id = saas.scholar_id
INNER JOIN (SELECT id, name FROM subject WHERE name IN ('Literacy', 'Mathematics', 'Science', 'History')) AS s
    ON s.id = saas.subject_id
WHERE saas.reference_date = '{target_date}'::date
    AND '{target_date}'::date >= ga.start_date
    AND '{target_date}'::date <= COALESCE(ga.end_date, '{target_date}'::date)
'''


#scholar_by_culture_query
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


#scholar_query
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


#Academics

scholar_subject = read_sql(scholar_by_subject_query.format(target_date='2016-02-17'), conn_pgsql)
scholar_achieve = read_sql(scholar_achievement_query.format(target_date='2016-02-17'), conn_pgsql)
scholar = merge(scholar_subject,scholar_achieve, on = ['scholar_id','subject', 'grade'])

np.unique(scholar.teacher)

Academics = scholar.loc[scholar.teacher.eq('Seagrave, Candice')]
Academics
np.unique(Academics.teacher)

Academics
round(Academics['achievement'].median(), 2)


#Culture 2015-2016

scholar_culture = read_sql(scholar_by_culture_query.format(target_date='2015-11-13'), conn_pgsql)
scholar_info = read_sql(scholar_query.format(target_date='2015-11-13'), conn_pgsql)
scholar_cult = merge(scholar_culture,scholar_info, on = ['scholar_id','grade'])
scholar_culture = scholar_cult[['scholar_id','scholar','school_name','school', 'grade', 'subject','teacher', 'achievement']]


scholar_culture_1 = scholar_culture.loc[scholar_culture.teacher.eq('Seagrave, Candice')]
scholar_culture_1
round(scholar_culture_1['achievement'].median(), 2)