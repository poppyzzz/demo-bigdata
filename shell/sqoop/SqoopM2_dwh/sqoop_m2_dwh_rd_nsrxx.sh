#!/bin/bash
if [ $# -lt 2 ]; then
sql="select  nsr.nsrsbh,nsr.nsrmc,max(t.yxqq) as yxqq,max(t.yxqz) yxqz from rd_nsrzgxx_jgb t,dj_nsrxx nsr where nsr.djxh=t.djxh and t.nsrzglx_dm in(201,202,203) and t.zfbz_1='N' and  \$CONDITIONS group by nsr.nsrsbh,nsr.nsrmc"
else
sql="select  nsr.nsrsbh,nsr.nsrmc,max(t.yxqq) as yxqq,max(t.yxqz) yxqz from rd_nsrzgxx_jgb t,dj_nsrxx nsr where t.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and nsr.djxh=t.djxh and t.nsrzglx_dm in(201,202,203) and t.zfbz_1='N' and  \$CONDITIONS group by nsr.nsrsbh,nsr.nsrmc"
fi
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh RD_NSRXX "$sql"