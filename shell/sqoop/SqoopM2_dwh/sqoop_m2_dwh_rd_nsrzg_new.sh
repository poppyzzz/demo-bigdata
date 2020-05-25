#!/bin/bash
if [ $# -lt 2 ]; then 
#sql="select * from RD_NSRZGXX_JGB where 1=1 and \$CONDITIONS"
sql="select a.*,nvl(b.shxydm, b.nsrsbh) as NSRDZDAH,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from RD_NSRZGXX_JGB a left join dj_nsrxx b on a.djxh=b.djxh where 1=1 and \$CONDITIONS"
else
#sql="select * from RD_NSRZGXX_JGB where lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
sql="select a.*,nvl(b.shxydm, b.nsrsbh) as NSRDZDAH,b.ZGSWJ_DM as SWJG_DM,b.ZGSWJ_DM as NSR_SWJG_DM from RD_NSRZGXX_JGB a left join dj_nsrxx b on a.djxh=b.djxh where a.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
fi
map=SJZZRQ=STRING,SJTB_SJ=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} M2_DWH RD_NSRZG "$sql" $map
