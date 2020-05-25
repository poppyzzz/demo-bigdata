#!/bin/bash
sql="select * from hx_dj.dj_ygzsffxxxdjb where lrrq between to_date('$1', 'yyyy-mm-dd') and to_date('$2 23:59:59', 'yyyy-mm-dd hh24:mi:ss') and  \$CONDITIONS"
map=SZDJRQ=STRING,NRYGZSDRQ=STRING,LRRQ=STRING,XGRQ=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh hx_33010 m2_dwh DJ_YGZSFFXXXDJB "$sql"
