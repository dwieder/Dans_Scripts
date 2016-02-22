from sqlalchemy import create_engine
from pandas import read_sql
import pandas as pd
from pandas import merge
import numpy as np
import datetime
from datetime import date



pd.set_option('display.expand_frame_repr',False)

conn_mssql = create_engine('mssql+pymssql://swheeler:welcome@192.168.150.134:1433/SCN')
conn_mysql = create_engine('mysql+pymysql://dna:Harlem.15@192.168.150.159:3306/dna')
conn_pgsql = create_engine('postgresql://admin:sacharters123@192.168.150.159:5432/smsport')




query='''

select al.rowid, ad.OldValue, ad.NewValue, al.ModifiedDate
from auditlog al
inner join auditdetail ad on al.id=ad.auditlogid
where al.tablename='ScholarForm' and ad.FieldName='Received' and al.rowid in (926243, 926247, 926248, 927026, 928197, 928199, 928445, 928445, 929129, 929129, 929573, 929573, 929968, 930054, 930055, 930461, 930463, 930465, 931430, 931429, 1054745, 1054746, 932791, 933156, 933201, 933201, 933211, 933211, 933685, 937499, 937499, 937563, 937871, 938284, 938298, 938987, 939844, 939853, 940538, 940538, 940694, 940694, 940694, 940694, 940800, 940800, 941538, 942082, 942301, 944904, 945014, 945015, 945170, 945926, 951000, 954492, 958319, 958319, 958319);


'''

df = read_sql(query, conn_mssql)



df['ModifiedDate'] = pd.to_datetime(df['ModifiedDate']).apply(lambda x: x.date())


df

df.loc[df.NewValue.eq('True')]




def query3():

    query2='''
    select al.rowid, ad.OldValue, ad.NewValue, al.ModifiedDate
    from auditlog al
    inner join auditdetail ad on al.id=ad.auditlogid
    where al.rowid=1212903

    and NewValue='true';'''

    df = read_sql(query2, conn_mssql)

    df['ModifiedDate'] = pd.to_datetime(df['ModifiedDate']).apply(lambda x: x.date())

    return df

query3()


def query4():

    query5='''
    select sf.id, sf.scholarid, sft.name, sf.received,  coalesce( sf.receiveddate,sfd.UpdatedOn), modifieddate
    from scholarform sf
    inner join scholarformdocument sfd on sf.id=sfd.scholarformid
    inner join scholarformtype sft on sft.id=sf.scholarformtypeid
    inner join auditlog al on al.rowid=sf.id
    inner join auditdetail ad on ad.auditlogid=al.id
    where sf.received='true' and sf.academicyearid=395 and NewValue='True'
    order by scholarid, sft.name;'''

    df = read_sql(query5, conn_mssql)

    df['ModifiedDate'] = pd.to_datetime(df['ModifiedDate']).apply(lambda x: x.date())

    return df

query4()


read_sql('select * from scholarform where scholarid=2381 and scholarformtypeid=423;', conn_mssql)