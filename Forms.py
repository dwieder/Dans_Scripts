from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np
import re
from pandas import ExcelWriter
from pandas import ExcelFile

pd.set_option('display.expand_frame_repr',False)


conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')



#scholar_per_school_query
scholars = '''
            select sc.name,  count(sa.scholar_id) as number_of_scholars_per_school
            from school_assignment sa
            left join scholar s on s.id=sa.scholar_id
            left join school sc on sc.id=sa.school_id
            where sa.academic_year_id=395 and s.withdrawal_reason_id is null
            group by sc.name
            order by sc.name'''


####Scholar Regular Forms

#forms_query
forms ='''
        select name, id, scholar_id,  count(scholar_form_type_id) :: int as scholar_count_forms, received_date
        from
            (select ss.name,ss.id, sf.scholar_id,  sf.scholar_form_type_id, sft.name as form_name, sf.received_date
            from scholar_form sf
            inner join scholar s on s.id=sf.scholar_id
            inner join scholar_form_type sft on sft.id=sf.scholar_form_type_id
            inner join school_assignment sa on sa.scholar_id=sf.scholar_id
            inner join school ss on ss.id=sa.school_id
            where  received='true' and sft.is_mandatory='true'  and  withdrawal_reason_id is null and withdrawn_without_attending='false'  and scholar_form_type_id not in (422,423, 424) and sft.required_frequency=1
            and sf.received_date is not null
            group by ss.name,ss.id, sf.scholar_id,  sf.scholar_form_type_id, sft.name, sf.received_date
            UNION
            select ss.name,ss.id, sf.scholar_id,  sf.scholar_form_type_id, sft.name as form_name, sf.received_date
            from scholar_form sf
            inner join scholar s on s.id=sf.scholar_id
            inner join scholar_form_type sft on sft.id=sf.scholar_form_type_id
            inner join school_assignment sa on sa.scholar_id=sf.scholar_id
            inner join school ss on ss.id=sa.school_id
            where sf.academic_year_id=395 and sa.academic_year_id=395 and received='true' and sft.is_mandatory='true'  and  withdrawal_reason_id is null and withdrawn_without_attending='false'  and scholar_form_type_id not in (422, 423, 424)
            and sf.received_date is not null
            group by ss.name,ss.id, sf.scholar_id,  sf.scholar_form_type_id, sft.name, sf.received_date) as forms_data
        group by name, id, scholar_id, received_date
        order by name, id, scholar_id;'''

#Identifying number of scholar per school
        scholars_per_school = read_sql(scholars, conn_pgsql)


#Identifying scholars with complete regular forms
        scholar_forms = read_sql(forms, conn_pgsql)

        len(np.unique(scholar_forms.name))

        scholar_forms.shape

        scholar_complete_forms = scholar_forms.loc[scholar_forms.scholar_count_forms.eq(8)]

        scholar_complete_forms.shape

        scholar_complete_forms_school = scholar_complete_forms[['name','scholar_id']]

        scholar_complete_forms_school.shape

        scholar_complete_forms_school = scholar_complete_forms_school.groupby('name')['scholar_id'].count().to_frame('complete_scholar_forms').reset_index()



#####Scholar Medical Forms

###scholar_form_allergy

        #find all scholars in scholar_form table with allergy form
        allergy_forms ='''
        select sm.scholar_id,
        scholar_form_type_id as maf_allergy,
        case
        when received='true' then 1
        when received='false' then 0
        end as maf_allergy_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=430 and condition_type=1 and medication_id is not null
        group by sm.scholar_id, received ,scholar_form_type_id'''

        #find all scholars with allergy medications
        allergy_meds='''
        select scholar_id
        from scholar_medical_condition
        where condition_type=1 and medication_id is not null
        group by scholar_id'''


        len(scholar_allergy_forms) = read_sql(allergy_forms, conn_pgsql)
        len(scholar_allergy_meds) = read_sql(allergy_meds, conn_pgsql)


        #scholars that do/not have allergy form
        scholar_forms_allergy = merge(scholar_allergy_forms, scholar_allergy_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_allergy.maf_allergy.fillna(430, inplace='True')

        #Replace all NaN values to 0 in Recieved column
        scholar_forms_allergy = scholar_forms_allergy.fillna(0)


###scholar_form_asthma

        #find all scholars in scholar_form table with allergy form
        asthma_forms ='''
        select sm.scholar_id,
        scholar_form_type_id as maf_asthma,
        case
        when received='true' then 1
        when received='false' then 0
        end as maf_asthma_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=445 and condition_type=2 and medication_id is not null
        group by sm.scholar_id, received ,scholar_form_type_id'''

        #find all scholars with allergy medications
        asthma_meds='''
        select scholar_id
        from scholar_medical_condition
        where condition_type=2 and medication_id is not null
        group by scholar_id'''


        scholar_asthma_forms = read_sql(asthma_forms, conn_pgsql)
        scholar_asthma_meds = read_sql(asthma_meds, conn_pgsql)


        #scholars that do/not have allergy form
        scholar_forms_asthma = merge(scholar_asthma_forms, scholar_asthma_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_asthma.maf_asthma.fillna(445, inplace='True')

        #Replace all NaN values to 0 in Recieved column
        scholar_forms_asthma = scholar_forms_asthma.fillna(0)



###scholar_form_diabetes
        #find all scholars in scholar_form table with allergy form
        diabetes_forms ='''
        select sm.scholar_id,
        scholar_form_type_id as maf_diabetes,
        case
        when received='true' then 1
        when received='false' then 0
        end as maf_diabetes_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=444 and condition_type=3 and medication_id is not null
        group by sm.scholar_id, received ,scholar_form_type_id'''

        #find all scholars with allergy medications
        diabetes_meds='''
        select scholar_id
        from scholar_medical_condition
        where condition_type=3 and medication_id is not null
        group by scholar_id'''


        scholar_diabetes_forms = read_sql(diabetes_forms, conn_pgsql)
        scholar_diabetes_meds = read_sql(diabetes_meds, conn_pgsql)


        #scholars that do/not have allergy form
        scholar_forms_diabetes = merge(scholar_diabetes_forms, scholar_diabetes_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_diabetes.maf_diabetes.fillna(445, inplace='True')

        #Replace all NaN values to 0 in Recieved column
        scholar_forms_diabetes = scholar_forms_diabetes.fillna(0)


###scholar_form_other_conditions
        #find all scholars in scholar_form table with allergy form
        other_conditions_forms ='''
        select sm.scholar_id,
        scholar_form_type_id as maf_other_conditions,
        case
        when received='true' then 1
        when received='false' then 0
        end as maf_other_conditions_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=446 and condition_type=4 and medication_id is not null
        group by sm.scholar_id, received ,scholar_form_type_id'''

        #find all scholars with allergy medications
        other_conditions_meds='''
        select scholar_id
        from scholar_medical_condition
        where condition_type=4 and medication_id is not null
        group by scholar_id'''


        scholar_other_conditions_forms = read_sql(other_conditions_forms, conn_pgsql)
        scholar_other_conditions_meds = read_sql(other_conditions_meds, conn_pgsql)


        #scholars that do/not have allergy form
        scholar_forms_other_conditions = merge(scholar_other_conditions_forms, scholar_other_conditions_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_other_conditions.maf_other_conditions.fillna(446, inplace='True')


        #Replace all NaN values to 0 in Recieved column
        scholar_forms_other_conditions = scholar_forms_other_conditions.fillna(0)


###scholar_form_self_administer
        self_administer_form ='''
        select sm.scholar_id,
        scholar_form_type_id as self_administer,
        case
        when received='true' then 1
        when received='false' then 0
        end as self_administer_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        left join scholar s on s.id=sf.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=433  and medication_id is not null and s.withdrawal_reason_id is null and is_deleted='false' and sm.self_medicate='true'
        group by sm.scholar_id, received ,scholar_form_type_id'''


        self_administer_meds = '''
        select scholar_id
        from scholar_medical_condition smc
        inner join scholar s on smc.scholar_id=s.id
        where s.withdrawal_reason_id is null and smc.medication_id is not null and smc.self_medicate='true' and is_deleted='false'
        group by smc.scholar_id;'''


        scholar_self_administer_forms = read_sql(self_administer_form, conn_pgsql)
        scholar_self_administer_meds = read_sql(self_administer_meds, conn_pgsql)



        #scholars that do/not have allergy form
        scholar_forms_self_administer = merge(scholar_self_administer_forms, scholar_self_administer_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_self_administer.self_administer.fillna(433, inplace='True')


        #Replace all NaN values to 0 in Recieved column
        scholar_forms_self_administer =  scholar_forms_self_administer.fillna(0)



###scholar_form_self_administer
        off_campus_form ='''
        select sm.scholar_id,
        scholar_form_type_id as off_campus,
        case
        when received='true' then 1
        when received='false' then 0
        end as off_campus_received
        from  scholar_medical_condition sm
        left join scholar_form sf  on sf.scholar_id=sm.scholar_id
        left join scholar s on s.id=sf.scholar_id
        where  academic_year_id=395 and scholar_form_type_id=434  and medication_id is not null and s.withdrawal_reason_id is null and is_deleted='false' and sm.self_medicate='false' and sm.self_carry='false'
        group by sm.scholar_id, received ,scholar_form_type_id'''


        off_campus_meds = '''
        select scholar_id
        from scholar_medical_condition smc
        inner join scholar s on smc.scholar_id=s.id
        where s.withdrawal_reason_id is null and smc.medication_id is not null and is_deleted='false' and smc.self_medicate='false' and smc.self_carry='false'
        group by smc.scholar_id;'''


        scholar_off_campus_forms = read_sql( off_campus_form, conn_pgsql)
        scholar_off_campus_meds = read_sql(off_campus_meds, conn_pgsql)



        #scholars that do/not have allergy form
        scholar_forms_off_campus = merge(scholar_off_campus_forms, scholar_off_campus_meds, how='outer',  left_on=['scholar_id'], right_on=['scholar_id'])

        #Repplace NaN Values with Scholar_form_Type_Id
        scholar_forms_off_campus.off_campus.fillna(434, inplace='True')


        #Replace all NaN values to 0 in Recieved column
        scholar_forms_off_campus =  scholar_forms_off_campus.fillna(0)



##Merging all scholar medical forms data frames

        scholar_off_campus_self_administer = merge(scholar_forms_self_administer, scholar_forms_off_campus,  on=['scholar_id'], how='outer')

        scholar_other_diabetes= merge(scholar_forms_other_conditions, scholar_forms_diabetes,  on=['scholar_id'], how='outer')
        scholar_asthma_allergy = merge(scholar_forms_asthma, scholar_forms_allergy,  on=['scholar_id'], how='outer')
        scholar_asthma_allergy_other_diabetes = merge(scholar_asthma_allergy, scholar_other_diabetes,  on=['scholar_id'], how='outer')
        scholar_medical_forms = merge(scholar_asthma_allergy_other_diabetes, scholar_off_campus_self_administer ,  on=['scholar_id'], how='outer')


        scholar_medical_forms = scholar_medical_forms[['scholar_id','maf_asthma_received','maf_allergy_received', 'maf_other_conditions_received', 'maf_diabetes_received', 'self_administer_received', 'off_campus_received']]


        scholar_medical_forms_1 = scholar_medical_forms.set_index('scholar_id').mean(axis=1).to_frame('complete_scholar_medical_forms').reset_index()


#merge scholar regular forms with scholar medical forms


        scholar_complete_forms_school = scholar_complete_forms[['name','scholar_id']]
        complete_forms_merge = merge(scholar_complete_forms_school, scholar_medical_forms_1, on=['scholar_id'], how='left')


#Identfying scholars with complete forms profile including medical
        complete_forms = complete_forms_merge.loc[complete_forms_merge.complete_scholar_medical_forms.isnull() | complete_forms_merge.complete_scholar_medical_forms.eq(1)]

#Count of scholars of complete forms profile by school
        complete_forms_school = complete_forms.groupby('name')['scholar_id'].count().to_frame('complete_scholar_forms').reset_index()
        complete_forms_by_school = merge(scholars_per_school, complete_forms_school, on=['name'])


#Percent of scholar with complete forms profiles by school
        complete_forms_by_school['percent_complete_forms'] = complete_forms_by_school.complete_scholar_forms.astype(float) / complete_forms_by_school.number_of_scholars_per_school.astype(float)
        complete_forms_by_school['percent_complete_forms'] = complete_forms_by_school['percent_complete_forms'].apply(lambda x: '{:0.2f}'.format(x * 100))


