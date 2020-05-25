#!/bin/bash
if [ $# -lt 2 ]; then
sql="select * from hx_fp.fp_ly_mx t where \$CONDITIONS"
else
sql="select * from hx_fp.fp_ly_mx t where t.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
fi
map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING,FPSL=BIGINT
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh FP_LY_MX_ALL "$sql" $map
