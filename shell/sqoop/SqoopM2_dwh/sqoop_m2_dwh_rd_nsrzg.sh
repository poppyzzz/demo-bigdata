#!/bin/bash
if [ $# -lt 2 ]; then 
sql="select * from hx_rd.RD_NSRZGXX_JGB where 1=1 and \$CONDITIONS"
#sql="select a.*,a.djxh as NSRDZDAH,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from RD_NSRZGXX_JGB a left join dj_nsrxx b on a.djxh=b.djxh where 1=1 and \$CONDITIONS"
else
sql="select * from hx_rd.RD_NSRZGXX_JGB where lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
#sql="select a.*,a.djxh as NSRDZDAH,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from RD_NSRZGXX_JGB a left join dj_nsrxx b on a.djxh=b.djxh where a.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
fi
map=SJZZRQ=STRING,SJTB_SJ=STRING,DJXH=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh RD_NSRZG "$sql" $map 
