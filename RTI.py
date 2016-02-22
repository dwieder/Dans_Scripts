from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np
from pandas import Series
import datetime as dt
import re
import calendar


pd.set_option('display.expand_frame_repr',False)

conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')



rti_query = '''
WITH rti as (
        WITH sped_rti_math AS (
                SELECT
                sch.name,
                sc.nickname,
                ca.id,
                ca.scholar_id,
                CASE
                        WHEN sb.name is not null THEN 'Yes'ELSE 'No'
                END AS rti_math,
                ca.start_date,
                ca.end_date,
                sc.grade
                FROM school_class sc
                INNER JOIN  subject sb ON sc.subject_id=sb.id
                INNER JOIN class_assignment ca ON sc.id=ca.school_clASs_id
                INNER JOIN school sch ON sch.id=sc.school_id
                WHERE sc.school_id IN (11414, 11390) AND sc.subject_id=10038
        ), sped_rti_ela AS (
                SELECT
                sch.name,
                sc.nickname,
                ca.id,
                ca.scholar_id,
                CASE
                        WHEN sb.name is not null THEN 'Yes'ELSE 'No'
                END AS rti_ela,
                ca.start_date,
                ca.end_date,
                sc.grade
                FROM school_class sc
                INNER JOIN  subject sb ON sc.subject_id=sb.id
                INNER JOIN class_assignment ca ON sc.id=ca.school_clASs_id
                INNER JOIN school sch ON sch.id=sc.school_id
                WHERE sc.school_id IN (11414, 11390) AND sc.subject_id=10039
        ), scholars AS (
                SELECT
                sch.name,
                ca.scholar_id,
                sc.subject_id,
                ca.start_date,
                ca.end_date,
                sc.grade
                FROM school_class sc
                INNER JOIN  subject sb ON sc.subject_id=sb.id
                INNER JOIN class_assignment ca ON sc.id=ca.school_clASs_id
                INNER JOIN school sch ON sch.id=sc.school_id
                INNER JOIN scholar ss on ss.id=ca.scholar_id
                WHERE sc.school_id IN (11414, 11390) AND sc.subject_id IN (10039,10038) and ss.withdrawal_reason_id is null
        ), sped AS (
                SELECT
                scholar_id,
                CASE
                        WHEN iep_status=0 THEN 'iep_graduate' ELSE 'hAS_iep'
                END AS iep_status
                FROM
                (
                SELECT
                scholar_id,
                SUM(iep_status) AS iep_status
                FROM
                (
                SELECT
                sp.scholar_id,
                sp.type,
                sp.start_date,
                sp.end_date,
                CASE
                        WHEN sp.end_date IS NULL THEN 1
                        ELSE 0
                END AS iep_status
                FROM sped sp
                INNER JOIN scholar s
                        ON s.id=sp.scholar_id
                INNER JOIN school_ASsignment sa
                        ON sp.scholar_id=sa.scholar_id
                WHERE s.withdrawal_reason_id is null AND sa.school_id IN (11414, 11390) AND sa.academic_year_id=395
                GROUP BY sp.scholar_id,sp.start_date,sp.end_date,sp.type
                ORDER BY sp.scholar_id) AS sp
                GROUP BY scholar_id) AS sped
        )
        SELECT
        sc.name,
        CONCAT(p.first_name,' ',p.last_name) AS scholar_name,
        s.scholar_id,
        s.grade,
        CASE
                WHEN rti_ela is not null THEN 'Yes' ELSE 'No'
        END AS rti_ela,
        CASE
                WHEN sre.nickname is null THEN 'N/A' ELSE sre.nickname
        END AS rti_ela_class_name,
        sre.start_date AS rti_ela_start_date,
        sre.end_date AS rti_ela_end_date,
        CASE
                WHEN rti_math is not null THEN 'Yes' ELSE 'No'
        END AS rti_math,
        CASE
                WHEN srm.nickname is null THEN 'N/A' ELSE srm.nickname
        END AS rti_math_class_name,
        srm.start_date AS rti_math_start_date,
        srm.end_date AS rti_math_end_date,
        CASE
                WHEN iep_status='hAS_iep' THEN 'Yes' ELSE 'No'
        END AS HAS_IEP,
        CASE
                WHEN iep_status='iep_graduate' THEN 'Yes' ELSE 'No'
        END AS IEP_Graduate
        FROM scholars AS s
        LEFT JOIN sped_rti_ela AS sre
                ON sre.scholar_id=s.scholar_id
        LEFT JOIN sped_rti_math AS srm
                ON srm.scholar_id=s.scholar_id
        LEFT JOIN sped AS sp
                ON sp.scholar_id=s.scholar_id
        INNER JOIN school_ASsignment AS sa
                ON s.scholar_id=sa.scholar_id
        INNER JOIN school AS sc
                ON sc.id=sa.school_id
        INNER JOIN person AS p
                ON s.scholar_id=p.id
        WHERE sa.academic_year_id=395
        GROUP BY s.scholar_id, s.grade, sre.nickname, rti_ela, sre.end_date,sre.start_date, srm.nickname, rti_math, srm.start_date, srm.end_date, iep_status,sc.name, p.first_name, p.last_name
        ORDER BY sc.name, s.grade, s.scholar_id
), scholar_achievement_math as (
        WITH decayed_scores AS (
            SELECT
                qqs.subject_id,
                qqs.assessment_id,
                qqs.assessment_question_id,
                exp(1.0)^((ln(0.5) / 28) * (CURRENT_DATE - a.due_date)) * qqs.score::float AS weight
            FROM question_quality_scores AS qqs
            INNER JOIN assessment AS a
                ON qqs.assessment_id = a.id
            INNER JOIN assessment_type AS at
                ON a.assessment_type_id = at.id
            WHERE a.due_date <= CURRENT_DATE
                AND a.due_date >= (CURRENT_DATE - 180)
                AND at.description NOT IN (
                    'Incoming Scholar Assessment', 'Math Olympiad Contest', 'NHM', 'Weekly Spelling',
                    'Spelling and Vocabulary Quiz', 'Spelling', 'Formal NHM')
                AND a.name NOT LIKE '%%OPTIONAL%%' and at.subject_id=7
        ), academic_achievement_snapshot AS (
            SELECT
                ds.subject_id,
                saap.scholar_id,
                CURRENT_DATE AS reference_date,
                (SUM(ds.weight * saap.percent_correct) + 0.025) / (SUM(ds.weight) + 0.05) AS score
            FROM scholar_assessment_answer_percent AS saap
            INNER JOIN decayed_scores AS ds
                ON ds.assessment_id = saap.assessment_id
                AND ds.assessment_question_id = saap.assessment_question_id
            WHERE NOT EXISTS (
                SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = saap.scholar_id AND w.final_date <= CURRENT_DATE)
            GROUP BY ds.subject_id, saap.scholar_id
        )
        SELECT
            saas.scholar_id,
            s.name AS subject,
            ga.grade,
            ROUND(CAST (saas.score as numeric), 2) AS achievement
        FROM academic_achievement_snapshot AS saas
        INNER JOIN grade_assignment AS ga
            ON ga.scholar_id = saas.scholar_id
        INNER JOIN (SELECT id, name FROM subject WHERE name IN ('Literacy', 'Mathematics', 'Science', 'History')) AS s
            ON s.id = saas.subject_id
        WHERE saas.reference_date = CURRENT_DATE
            AND CURRENT_DATE >= ga.start_date
            AND CURRENT_DATE <= COALESCE(ga.end_date, CURRENT_DATE)
 ), scholar_achievement_ela as (
        WITH decayed_scores AS (
            SELECT
                qqs.subject_id,
                qqs.assessment_id,
                qqs.assessment_question_id,
                exp(1.0)^((ln(0.5) / 28) * (CURRENT_DATE - a.due_date)) * qqs.score::float AS weight
            FROM question_quality_scores AS qqs
            INNER JOIN assessment AS a
                ON qqs.assessment_id = a.id
            INNER JOIN assessment_type AS at
                ON a.assessment_type_id = at.id
            WHERE a.due_date <= CURRENT_DATE
                AND a.due_date >= (CURRENT_DATE - 180)
                AND at.description NOT IN (
                    'Incoming Scholar Assessment', 'Math Olympiad Contest', 'NHM', 'Weekly Spelling',
                    'Spelling and Vocabulary Quiz', 'Spelling', 'Formal NHM')
                AND a.name NOT LIKE '%%OPTIONAL%%' and at.subject_id=6
        ), academic_achievement_snapshot AS (
            SELECT
                ds.subject_id,
                saap.scholar_id,
                CURRENT_DATE AS reference_date,
                (SUM(ds.weight * saap.percent_correct) + 0.025) / (SUM(ds.weight) + 0.05) AS score
            FROM scholar_assessment_answer_percent AS saap
            INNER JOIN decayed_scores AS ds
                ON ds.assessment_id = saap.assessment_id
                AND ds.assessment_question_id = saap.assessment_question_id
            WHERE NOT EXISTS (
                SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = saap.scholar_id AND w.final_date <= CURRENT_DATE)
            GROUP BY ds.subject_id, saap.scholar_id
        )
        SELECT
            saas.scholar_id,
            s.name AS subject,
            ga.grade,
            ROUND(CAST (saas.score as numeric), 2) AS achievement
        FROM academic_achievement_snapshot AS saas
        INNER JOIN grade_assignment AS ga
            ON ga.scholar_id = saas.scholar_id
        INNER JOIN (SELECT id, name FROM subject WHERE name IN ('Literacy', 'Mathematics', 'Science', 'History')) AS s
            ON s.id = saas.subject_id
        WHERE saas.reference_date = CURRENT_DATE
            AND CURRENT_DATE >= ga.start_date
            AND CURRENT_DATE <= COALESCE(ga.end_date, CURRENT_DATE)
 )
        SELECT
        r.name,
        r.scholar_name,
        r.scholar_id,
        r.grade,
        r.rti_ela,
        COALESCE(r.rti_ela_start_date::text, 'N/A') AS rti_ela_start_date,
        r.rti_ela_end_date,
        r.rti_math,
        COALESCE(r.rti_math_start_date::text, 'N/A') AS rti_math_start_date,
        r.rti_math_end_date,
        r.HAS_IEP,
        IEP_Graduate,
        sm.achievement as math_achievement_score,
        se.achievement as ela_achievement_score
        FROM rti as r
        LEFT JOIN scholar_achievement_math AS sm
                ON r.scholar_id=sm.scholar_id
        LEFT JOIN scholar_achievement_ela AS se
                ON r.scholar_id=se.scholar_id
        ORDER BY r.name, r.scholar_name'''


rti = read_sql(rti_query, conn_pgsql)


date = dt.datetime.today().strftime("%m-%d-%Y")

output_string = 'RTI_Report' + '_' + date + '.csv'

rti.to_csv('/Users/dwieder/Desktop/RTI/' + output_string, index=False)