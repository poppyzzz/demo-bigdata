#!/bin/bash
if [ $# -lt 2 ]; then 
sql="select * from dj_nsrxx_zjh where 1=1 and length(nsrsbh)>=14 and \$CONDITIONS"
else
sql="select * from dj_nsrxx_zjh where length(nsrsbh)>=14 and lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
fi
bash SqoopJdbc.sh M2_DWH m2_dwh DJ_NSRXX_ZJH "$sql" $map
