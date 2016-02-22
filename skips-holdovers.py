from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np

pd.set_option('display.expand_frame_repr',False)
conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')
conn_mysql = create_engine('mysql+pymysql://dna:Harlem.15@192.168.150.159:3306/dna')
conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport2')



skips = '''
        select skip_data.academic_year_id, cast(grade_skip as int) as grade, count(skip_data.scholar_id) as skipped_scholars
        from
            (Select ga.scholar_id , ga.academic_year_id, (ga.grade-ga.grade_shift) as grade_skip
            from grade_assignment ga
            left join class_assignment ca on ca.scholar_id=ga.scholar_id
            left join school_assignment sa on ga.scholar_id=sa.scholar_id
            where COALESCE(year_shift, 0) < COALESCE(grade_shift, 0)
            and ga.end_date is not null
            and  trial = False
            AND reversal = False
            AND (ca.start_date BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE) OR COALESCE(ca.end_date, CURRENT_DATE) BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE))
            AND (ga.start_date BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE) OR COALESCE(ga.end_date, CURRENT_DATE) BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE))
            AND ga.academic_year_id in (394, 387, 386, 385, 384)
            group by ga.academic_year_id, grade_skip, ga.scholar_id) as skip_data
        group by skip_data.academic_year_id, grade_skip
        order by skip_data.academic_year_id, grade_skip; '''

skips = read_sql(skips, conn_pgsql)
skips



holdovers = '''select academic_year_id, cast(grade_holdover as int) as grade, count(scholar_id) holdover_scholars
            from
        (Select ga.scholar_id , ga.academic_year_id, (ga.grade-ga.grade_shift) as grade_holdover
        from grade_assignment ga
        left join class_assignment ca on ca.scholar_id=ga.scholar_id
        left join school_assignment sa on ga.scholar_id=sa.scholar_id
        where COALESCE(year_shift, 0) > COALESCE(grade_shift, 0)
        and ga.end_date is not null
        and  trial = False
        AND reversal = False
        AND (ca.start_date BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE) OR
        COALESCE(ca.end_date, CURRENT_DATE) BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE))
        AND (ga.start_date BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE) OR COALESCE(ga.end_date, CURRENT_DATE) BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE))
        AND ga.academic_year_id in (394, 387, 386, 385, 384)
        group by ga.academic_year_id, grade_holdover, ga.scholar_id) as holdover
        group by academic_year_id, grade_holdover
        order by academic_year_id, grade_holdover;'''


holdovers = read_sql(holdovers, conn_pgsql)
holdovers



skips_holdovers = merge(holdovers, skips, how='left', on = ['academic_year_id', 'grade'])
skips_holdovers


scholar_count = '''select academic_year_id, grade, count(scholar_id) as scholars
                  from grade_assignment
                  where academic_year_id in (395,394, 387, 386, 385, 384)
                  group by academic_year_id, grade;'''

scholar_count = read_sql(scholar_count, conn_pgsql)
scholar_count



skips_holdovers_scholars = merge(skips_holdovers, scholar_count, on = ['academic_year_id', 'grade'])
skips_holdovers_scholars


skips_holdovers_scholars.to_csv('skips vs. holdovers.csv')



read_sql('select * from academic_year', conn_pgsql)





Scholar_skips= '''
    SELECT
    s.name,
    skip_data.grade,
    academic_year,
    s.id,
    COUNT(skip_data.scholar_id) as number_of_skips
    FROM (
            SELECT
                sa.school_id,
                (ga.grade - ga.grade_shift)::text AS grade,
                ga.scholar_id,
                ay.description AS academic_year,
                regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS skip
            FROM grade_assignment AS ga
            INNER JOIN academic_year AS ay
                ON ga.academic_year_id = ay.id
            INNER JOIN school_assignment sa on sa.scholar_id=ga.scholar_id and ga.academic_year_id=sa.academic_year_id
            WHERE
            trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) < COALESCE(grade_shift, 0)
            AND ga.academic_year_id in (387, 394)
            AND (ga.grade - ga.grade_shift) > 4
            AND sa.school_id<>11418
            ORDER BY ay.description
            ) AS skip_data
    INNER JOIN school s
            ON s.id=skip_data.school_id
    WHERE s.school_type=2
    GROUP BY name ,academic_year,s.id ,skip_data.grade
    ORDER BY s.name,  academic_year'''



Scholar_holdovers= '''
    SELECT
    s.name,
    skip_data.grade,
    academic_year,
    s.id,
    COUNT(skip_data.scholar_id) as number_of_skips
    FROM (
            SELECT
                sa.school_id,
                (ga.grade - ga.grade_shift)::text AS grade,
                ga.scholar_id,
                ay.description AS academic_year,
                regexp_replace((ga.grade - ga.grade_shift)::text || ' -> ' || ga.grade::text, '-[0-9]', '0') AS skip
            FROM grade_assignment AS ga
            INNER JOIN academic_year AS ay
                ON ga.academic_year_id = ay.id
            INNER JOIN school_assignment sa on sa.scholar_id=ga.scholar_id and ga.academic_year_id=sa.academic_year_id
            WHERE
            trial = False
            AND reversal = False
            AND COALESCE(year_shift, 0) > COALESCE(grade_shift, 0)
            AND ga.academic_year_id in (387, 394)
            AND (ga.grade - ga.grade_shift) > 4
            AND sa.school_id<>11418
            ORDER BY ay.description
            ) AS skip_data
    INNER JOIN school s
            ON s.id=skip_data.school_id
    WHERE s.school_type=2
    GROUP BY name ,academic_year,s.id ,skip_data.grade
    ORDER BY s.name,  academic_year'''







enrollment_query = '''
SELECT
    '{ref_date}' AS reference_date,
    regexp_replace(sch.name, '^Success Academy ', '') AS school_long,
    regexp_replace(sch.abbreviation, '^SA-', '') AS school_short,
    ga.grade,
    COUNT(DISTINCT s.id) AS n_scholars
FROM scholar AS s
INNER JOIN school_assignment AS sa
    ON sa.scholar_id = s.id
INNER JOIN grade_Assignment AS ga
    ON ga.scholar_id = s.id
INNER JOIN school AS sch
    ON sch.id = sa.school_id
WHERE '{ref_date}' BETWEEN sa.start_date AND COALESCE(sa.end_date, CURRENT_DATE)
    AND '{ref_date}' BETWEEN ga.start_date AND COALESCE(ga.end_date, CURRENT_DATE)
    AND NOT EXISTS (
        SELECT 1 FROM withdrawal AS w WHERE w.scholar_id = sa.scholar_id AND w.final_date <= '{ref_date}'
    )
GROUP BY school_short, school_long, grade
'''


df = read_sql(enrollment_query.format(ref_date='2015-06-22'), conn_pgsql)















