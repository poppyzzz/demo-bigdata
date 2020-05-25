#!/bin/bash
if [ $# -lt 2 ]; then 
sql="select a.NSRSBH,t.* from hx_fp.fp_ly t, hx_dj.DJ_NSRXX a where a.DJXH=t.DJXH and \$CONDITIONS"
else
sql="select t.*,a.NSRSBH from (select * from hx_fp.fp_ly t where t.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) t,hx_dj.DJ_NSRXX a where a.DJXH=t.DJXH and \$CONDITIONS"
fi
map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING,DJXH=STRING,LPSLSX=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh FP_LY_ALL "$sql" $map
