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
        CEILING((CURRENT_DATE - sch.start_date)/365.0)::text AS school_age,
        CASE
            WHEN st.type = 'Elementary' THEN 'ES'
            WHEN st.type = 'Middle' THEN 'MS'
            WHEN st.type = 'High' THEN 'HS'
        END AS school_type,
        ga.grade::text AS grade
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
        sp.group_size
    FROM sped_program AS sp
    INNER JOIN sped_program_type AS spt
        ON sp.program_type = spt.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    WHERE spt.type <> 'General Education'
        AND '{target_date}'::date BETWEEN sp.start_date AND COALESCE(sp.end_date, CURRENT_DATE)
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
        sp.group_size
    FROM sped_service AS sp
    INNER JOIN sped_service_type AS sst
        ON sp.service_type = sst.id
    INNER JOIN start_date AS sd
        ON sd.scholar_id = sp.scholar_id
    WHERE sst.type NOT LIKE 'At Risk%%'
        AND '{target_date}'::date BETWEEN sp.start_date AND COALESCE(sp.end_date, CURRENT_DATE)
    )
), all_sped AS (
    SELECT
        p.scholar_id,
        p.type,
        p.subject,
        p.frequency::text,
        p.duration::text,
        p.group_size::text
    FROM sped_raw AS p
), sped AS (
    SELECT
        asp.scholar_id,
        array_agg(DISTINCT asp.type) AS sped_nav,
        array_agg_mult(ARRAY[ARRAY[
            asp.type, asp.subject, asp.frequency::text, asp.duration::text, asp.group_size::text
        ]]) AS sped
    FROM all_sped AS asp
    GROUP BY asp.scholar_id
), rti AS (
    SELECT
        ca.scholar_id,
        array_agg(COALESCE(sub.name, 'None')) AS rti_nav,
        array_agg_mult(ARRAY[ARRAY[COALESCE(sub.name, 'None'), (COALESCE(ca.end_date, CURRENT_DATE) - ca.start_date)::text]]) AS rti
    FROM class_assignment AS ca
    INNER JOIN subject AS sub
        ON ca.subject_id = sub.id
    WHERE sub.name LIKE 'Intervention%%'
    AND '{target_date}'::date BETWEEN ca.start_date AND COALESCE(ca.end_date, CURRENT_DATE)
    GROUP BY ca.scholar_id
), ser AS (
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
    COALESCE(rti.rti, string_to_array('', '')) AS rti,
    COALESCE(rti.rti_nav, ARRAY['None']) AS rti_nav,
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
LEFT JOIN rti
    ON rti.scholar_id = s.scholar_id
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
    GROUP BY ds.subject_id, saap.scholar_id
)
SELECT
    saas.scholar_id,
    /*saas.reference_date,*/
    s.name AS subject,
    ga.grade,
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


scholar_subject_15 = read_sql(scholar_by_subject_query.format(target_date='2015-06-26'), conn_pgsql)
scholar_achieve_15 = read_sql(scholar_achievement_query.format(target_date='2015-06-26'), conn_pgsql)



scholar_15 = merge(scholar_subject_15,scholar_achieve_15, on = ['scholar_id', 'subject'])
scholar_15_suspended = scholar_15[['scholar_id','scholar', 'school_name', 'subjectx', 'achievement']]
scholar_15_suspended.head()


suspended_scholars_15 = [2211, 2250, 2333, 2482, 2547, 2712, 2786, 2895, 2926, 3213, 3250, 3260, 3262, 3293, 3371, 3397, 3408, 3460, 3580, 3647, 3660, 3742, 3772, 3799, 3817, 4014, 4068, 4096, 4237, 4311, 4329, 4577, 4639, 4685, 4887, 4929, 4956, 4980, 5005, 5074, 5200, 5203, 5300, 5371, 5471, 5479, 5551, 5578, 5612, 5660, 5964, 5973, 6010, 6044, 6141, 6165, 6178, 6324, 6632, 6695, 6707, 6719, 6900, 6974, 6983, 7012, 7055, 7076, 7226, 7276, 7286, 7333, 7386, 7869, 7964, 8061, 8103, 8181, 8342, 8407, 8451, 8470, 8542, 8629, 8645, 8681, 8711, 8767, 8807, 8843, 8879, 9039, 9104, 9241, 9261, 9317, 9322, 9365, 9400, 9473, 9649, 9775, 9827, 9930, 9968, 10036, 10048, 10117, 10203, 10317, 10361, 10417, 10427, 10441, 10476, 10537, 10660, 10671, 10681, 10885, 10957, 11022, 11089, 11108, 11142, 11148, 11176, 11214, 11228, 11265, 11269, 11353, 11450, 11858, 11931, 12085, 12345, 12435, 12437, 12480, 12632, 12658, 12684, 12768, 12824, 12889, 12899, 12930, 13262, 13276, 13363, 13592, 13699, 13739, 13772, 13775, 13798, 13840, 13871, 13992, 14141, 14161, 14243, 14315, 14393, 14412, 14469, 14486, 14520, 14530, 14552, 14577, 14596, 14690, 14693, 14780, 14825, 14932, 14954, 15060, 15117, 15213, 15250, 15290, 15383, 15388, 15430, 15478, 15519, 15555, 15567, 15605, 15720, 15787, 15833, 15957, 15975, 16011, 16093, 16209, 16340, 16440, 16449, 16467, 16499, 16583, 16682, 16722, 16856, 17112, 17174, 17249, 17355, 17372, 17471, 17487, 17558, 17622, 17687, 17753, 18149, 18161, 18380, 18397, 18544, 18571, 18593, 18637, 18643, 18693, 18739, 18741, 18978, 19041, 19069, 19091, 19127, 19191, 19230, 19330, 19338, 19485, 19565, 19582, 19678, 19734, 19801, 19830, 19879, 19922, 19955, 20029, 20031, 20059, 20082, 20111, 20149, 20228, 20264, 20323, 20325, 20405, 20434, 20436, 20492, 20505, 20811, 20905, 20958, 21158, 21254, 21364, 21410, 21454, 21488, 21663, 21735, 21788, 21803, 21909, 21932, 22028, 22103, 22111, 22213, 22298, 22326, 22358, 22439, 22453, 22553, 22585, 22590, 22667, 22701, 22714, 22782, 22828, 22869, 23062, 23223, 23314, 23437, 23446, 23496, 23562, 23755, 23793, 23873, 23876, 23888, 23914, 23970, 23988, 24090, 24127, 24213, 24281, 24336, 24446, 24562, 24578, 24580, 24625, 24774, 24930, 24958, 24997, 25055, 25289, 25324, 25447, 25480, 25614, 25661, 25728, 25845, 25924, 26070, 26146, 26338, 26349, 26500, 26569, 26571, 26621, 26655, 26666, 26698, 26707, 26785, 26827, 26842, 26845, 26866, 26899, 26940, 27003, 27067, 27073, 27101, 27130, 27404, 27507, 27518, 27533, 27536, 27585, 27883, 28195, 28200, 28243, 28289, 28313, 28357, 28363, 28459, 28474, 28493, 28695, 28701, 28711, 28747, 28749, 28794, 28894, 28922, 28935, 28983, 28999, 29016, 29068, 29085, 29154, 29217, 29314, 29319, 29332, 29562, 29719, 29801, 29931, 29978, 30004, 30118, 30139, 30146, 30169, 30209, 30214, 30220, 30378, 30434, 30453, 30514, 30677, 30704, 30849, 30896, 30901, 30943, 30947, 30963, 30986, 31139, 31262, 31323, 31373, 31382, 31404, 31412, 31458, 31518, 31535, 31987, 32016, 32060, 32132, 32160, 32254, 32323, 32507, 32509, 32600, 32723, 32754, 32779, 32798, 32801, 32810, 32853, 32917, 33123, 33266, 33301, 33366, 33428, 33458, 33460, 33583, 33650, 33763, 33812, 33815, 33865, 33945, 33956, 34057, 34137, 34200, 34219, 34284, 34291, 34396, 34554, 34629, 34653, 34673, 34728, 34762, 34781, 34810, 34864, 34901, 34923, 34949, 34965, 34990, 35036, 35086, 35461, 35950, 36031, 36051, 36134, 36156, 36211, 36220, 36300, 36315, 36330, 36348, 36362, 36409, 36462, 36491, 36520, 36526, 36532, 36654, 36688, 36814, 36979, 36988, 37024, 37118, 37170, 37349, 37358, 37429, 37502, 37707, 37725, 37799, 37815, 37900, 37980, 38018, 38079, 38245, 38305, 38346, 38371, 38425, 38474, 38569, 38650, 38674, 38760, 38906, 38993, 39044, 39078, 39195, 39389, 39400, 39456, 39467, 39750, 39870, 39930, 39941, 39982, 40050, 40055, 40114, 40124, 40135, 40623, 40650, 40668, 40692, 40699, 40731, 40762, 40867, 41008, 41113, 41639, 41703, 41715, 41751, 41776, 41801, 41904, 41999, 42080, 42133, 42136, 42186, 42198, 42285, 42390, 42563, 42618, 42628, 42638, 42828, 42862, 43210, 43272, 43278, 43354, 43418, 43466, 43470, 43510, 43589, 43622, 43671, 43673, 43867, 43881, 43949, 44020, 44022, 44140, 44205, 44212, 44337, 44355, 44404, 44641, 44725, 44949, 45026, 45184, 45271, 45272, 45369, 45443, 45466, 45492, 45527, 45568, 45633, 45642, 45679, 45699, 45747, 45771, 45800, 45879, 46080, 46092, 46209, 46255, 46321, 46448, 46468, 46498, 46659, 47037, 47239, 47279, 47482, 47521, 47708, 47737, 47749, 48021, 48033, 48087, 48135, 48169, 48236, 48288, 48509, 48579, 48828, 48994, 49009, 49321, 49367, 49392, 49416, 49509, 49566, 49639, 49715, 49886, 49895, 49928, 49983, 50102, 50389, 50393, 50453, 50529, 50544, 50568, 50731, 50751, 50760, 50835, 50886, 50969, 51121, 51154, 51189, 51244, 51250, 51292, 51387, 51449, 51470, 51506, 51527, 51781, 51873, 52053, 52106, 52135, 52159, 52173, 52246, 52272, 52277, 2210610249, 2210610347, 2210610451, 2210610485, 2210610517, 2210610538, 2210610546, 2300006105, 2300006176, 2300006223, 2300006301, 2300006330, 2300006405, 2300018990, 2300018992, 2300018994, 2300019021, 2300019038, 2300019065, 2300019086, 2300020061, 2300020063, 2300030297, 2300030345, 2300030351, 2300030366, 2300030392, 2300030470, 2300030480, 2300030488, 2300030495, 2300030506, 2300030516, 2300030526, 2300030567, 2300030569, 2300030575, 2300030591, 2300030599, 2300030619, 2300030627, 2300030632, 2300030663, 2300030670, 2300030757, 2300030767, 2300030791, 2300030848, 2300030882, 2300030895, 2300030944, 2300030974, 2300030980, 2300031020, 2300031089, 2300031227, 2300031232, 2300031234, 2300031241, 2300031243, 2300031356, 2300031362, 2300031366, 2300031377, 2300031392, 2300031454, 2300031503, 2300031550, 2300031553, 2300031556, 2300031696, 2300031707, 2300031726, 2300031739, 2300031751, 2300031807, 2300031826, 2300031866, 2300031913, 2300031944, 2300031970, 2300031972, 2300031984, 2300032094, 2300032147, 2300032250, 2300032272, 2300032321, 2300032339, 2300032346, 2300032348, 2300032367, 2300032472, 2300032637, 2300032657, 2300032695, 2300032808, 2300032827, 2300032839, 2300032875, 2300032887, 2300032940, 2300032983, 2300032996, 2300033023, 2300033151, 2300033184, 2300033211, 2300033217, 2300033229, 2300033247, 2300033252, 2300033278, 2300033280, 2300033355, 2300033364, 2300033373, 2300033413, 2300033528, 2300033531, 2300033533, 2300033535, 2300033544, 2300033559, 2300033585, 2300033591, 2300033602, 2300033612, 2300033614, 2300033691, 2300033706, 2300033747, 2300033753, 2300033760, 2300033766, 2300033772, 2300033780, 2300033793, 2300033873, 2300033890, 2300033901, 2300033947, 2300034070, 2300034086, 2300034115, 2300034129, 2300034137, 2300034174, 2300034191, 2300034379, 2300034405, 2300034410, 2300034468, 2300034476, 2300034482, 2300034545, 2300034571, 2300034595, 2300034603, 2300034606, 2300034712, 2300034726, 2300034739, 2300034741, 2300034787, 2300034798, 2300034805, 2300034824, 2300034831, 2300034853, 2300034881, 2300034946, 2300034964, 2300035015, 2300035017, 2300035056, 2300035060, 2300035062, 2300035065, 2300035123, 2300035141, 2300035154, 2300035158, 2300035171, 2300035173, 2300035258, 2300035265, 2300035360, 2300035373, 2300035376, 2300035440, 2300035456, 2300035481, 2300035527, 2300035529, 2300035538, 2300035557, 2300035574, 2300035632, 2300035669, 2300035802, 2300035805, 2300035889, 2300035904, 2300035971, 2300035973, 2300035986, 2300036073, 2300036154, 2300036173, 2300036201, 2300036228, 2300036233, 2300036237, 2300036375, 2300036446, 2300036448, 2300036468, 2300036470, 2300036528, 2300036547, 2300036584, 2300036587, 2300036600, 2300036607, 2300036646, 2300036656, 2300036665, 2300036670, 2300036683, 2300036732, 2300036869, 2300036895, 2300036911, 2300036923, 2300036984, 2300037004, 2300037038, 2300037093, 2300037101, 2300037161, 2300037209, 2300037223, 2300037227, 2300037259, 2300037272, 2300037274, 2300037316, 2300037411, 2300037461, 2300037481, 2300037503, 2300037588, 2300096187, 2300096271, 2300096277, 2300096331, 2300096396, 2300096447, 2300096469, 2300096545, 2300096657, 2300096692, 2300096694, 2300101554, 2300101620, 2300101660, 2300101688, 2300101738, 2300101837, 2300101969, 2300102056, 2300103076, 2300103085, 2300103100, 2300103117, 2300103224, 2300103238, 2300103245, 2300103286, 2300105833, 2300105850, 2300105878, 2300105899, 2300105980, 2300107239, 2300107294, 2300108396, 2300109380, 2300109416, 2300110949, 2300110966, 2300120564, 2300121094, 2300121492, 2300121500]

scholars_suspended = scholar_15.loc[scholar_15.scholar_id.isin(suspended_scholars_15)]
scholars_suspended

len(np.unique(scholars_suspended.scholar_id))

d = scholars_suspended.groupby('scholar_id').achievement.mean()
