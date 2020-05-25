#!/bin/bash
map=LRRQ=STRING,XGRQ=STRING,SJTB_SJ=STRING,DJXH=STRING
if [ $# -lt 2 ]; then 
sql="select * from (select rownum as ROWIDSTR,t.* from hx_dj.dj_nsrxx t where length(t.nsrsbh)>=14 and t.nsrzt_dm not in( '09','10') and t.kzztdjlx_dm<>'1500' and t.kqccsztdjbz='N') where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh_BigTable.sh ${m2_dwh_username} m2_dwh DJ_NSRXX "$sql" $map
else
sql="select rownum as ROWIDSTR,t.* from hx_dj.dj_nsrxx t where length(t.nsrsbh)>=14 and nsrzt_dm not in( '09','10') and kzztdjlx_dm<>'1500' and t.kqccsztdjbz='N' and t.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2','yyyy-mm-dd') and \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DJ_NSRXX "$sql" $map
fi

