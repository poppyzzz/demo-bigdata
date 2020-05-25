#!/bin/bash
sql="select * from dzdz_fpxx_hyzp2017 t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=KPYF=DOUBLE,HJJE=DOUBLE,SL=DOUBLE,SE=DOUBLE,JSHJ=DOUBLE
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_FPXX_HYZP "$sql" $map 

# sql="select * from dzdz_fpxx_hyzp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
# map=KPYF=DOUBLE,HJJE=DOUBLE,SL=DOUBLE,SE=DOUBLE,JSHJ=DOUBLE
# bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_FPXX_HYZP "$sql" $map 


