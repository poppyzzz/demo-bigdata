#!/bin/bash
map=ZCZB=DOUBLE,TZZE=DOUBLE,CYRS=DOUBLE,WJCYRS=DOUBLE,HHRS=DOUBLE,GDGRS=DOUBLE,ZRRTZBL=DOUBLE,WZTZBL=DOUBLE,LRRQ=STRING,XGRQ=STRING,DJXH=STRING
if [ $# -lt 2 ]; then 
sql="select * from (select rownum as ROWIDSTR,t.* from hx_dj.dj_nsrxx_kz t) where \$CONDITIONS"
#sql="select a.*,a.djxh as NSRDZDAH,b.hy_dm,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from dj_nsrxx_kz a,dj_nsrxx b where a.djxh=b.djxh and \$CONDITIONS"

bash ${UtilPath}/SqoopJdbc_M2_dwh_BigTable.sh ${m2_dwh_username} m2_dwh DJ_NSRXX_KZ "$sql" $map
else
sql="select rownum as ROWIDSTR,t.* from hx_dj.dj_nsrxx_kz t where lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
#sql="select a.*,nvl(b.shxydm, b.nsrsbh) as NSRDZDAH,b.hy_dm,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from dj_nsrxx_kz a,dj_nsrxx b where a.djxh=b.djxh and a.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"

bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DJ_NSRXX_KZ "$sql" $map
fi






