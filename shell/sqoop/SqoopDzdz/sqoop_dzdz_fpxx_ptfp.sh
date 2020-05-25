#!/bin/bash
sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_fpxx_ptfp2017 t where t.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
map=JE=DOUBLE,SE=DOUBLE,JSHJ=DOUBLE,KPJH=DOUBLE,KPYF=DOUBLE,CYCS=DOUBLE
bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_FPXX_PTFP "$sql" $map

# sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_fpxx_ptfp t where t.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
# map=JE=DOUBLE,SE=DOUBLE,JSHJ=DOUBLE,KPJH=DOUBLE,KPYF=DOUBLE,CYCS=DOUBLE
# bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_FPXX_PTFP "$sql" $map
