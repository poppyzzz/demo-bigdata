#!/bin/bash
sql="select * from dzdz_fpxx_jdcfp2017 t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
# sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_fpxx_jdcfp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
map=KPYF=DOUBLE,CJFY=DOUBLE,ZZSSL=DOUBLE,ZZSSE=DOUBLE,JSHJ=DOUBLE,RZDKL_RZYF=DOUBLE
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_FPXX_JDCFP "$sql" $map 

# sql="select * from dzdz_fpxx_jdcfp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
# map=KPYF=DOUBLE,CJFY=DOUBLE,ZZSSL=DOUBLE,ZZSSE=DOUBLE,JSHJ=DOUBLE,RZDKL_RZYF=DOUBLE
# bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_FPXX_JDCFP "$sql" $map 
