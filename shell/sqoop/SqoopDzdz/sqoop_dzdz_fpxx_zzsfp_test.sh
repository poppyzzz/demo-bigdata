#!/bin/bash
# sql="select * from dzdz_fpxx_zzsfp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"

#sql="select * from (select rownum as ROWIDSTR,t.* from dzdz.dzdz_fpxx_zzsfp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"

#DATE=$(date -d"$2" +"%Y%m")
sql="select * from (select rownum as ROWIDSTR,t.* from dzdz.dzdz_fpxx_zzsfp t where kpyf=$1) where \$CONDITIONS"
map=JE=double,SE=Double,JSHJ=DOUBLE,KPJH=DOUBLE,RZDKL_RZYF=DOUBLE,KPYF=STRING
bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh ${dzdz_username} dzdz DZDZ_FPXX_ZZSFP_TEST "$sql" $map

# sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_fpxx_zzsfp t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
# map=JE=double,SE=Double,JSHJ=DOUBLE,KPJH=DOUBLE,RZDKL_RZYF=DOUBLE
# bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_FPXX_ZZSFP "$sql" $map

