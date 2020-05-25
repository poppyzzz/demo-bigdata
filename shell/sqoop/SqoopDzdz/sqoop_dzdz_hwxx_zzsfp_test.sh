#!/bin/bash

#DATE=$(date -d"$2" +"%Y%m")

# sql="select t.* from dzdz_hwxx_zzsfpxx t inner join dzdz_fpxx_zzsfp f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"

#sql="select * from (select rownum as ROWIDSTR,t.* from dzdz.dzdz_hwxx_zzsfp02 t inner join dzdz.dzdz_fpxx_zzsfp02 f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"

#sql="select * from (select rownum as ROWIDSTR,t.* from dzdz.dzdz_hwxx_zzsfp_${DATE} t inner join dzdz.dzdz_fpxx_zzsfp_${DATE} f on t.fpdm=f.fpdm and t.fphm=f.fphm where kpyf=${DATE} ) where \$CONDITIONS"

sql="select * from (select rownum as ROWIDSTR,t.* from dzdz.dzdz_hwxx_zzsfp t where kpyf=$1) where \$CONDITIONS"
map=ID=DOUBLE,SL=DOUBLE,DJ=DOUBLE,JE=DOUBLE,SLV=DOUBLE,SE=DOUBLE,KPYF=STRING
#bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh ${dzdz_username} dzdz DZDZ_HWXX_ZZSFP "$sql" $map
#bash ${UtilPath}/SqoopJdbc_Dzdz_14.sh ${dzdz_14_username} dzdz DZDZ_HWXX_ZZSFP "$sql" $map
bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh ${dzdz_username} dzdz DZDZ_HWXX_ZZSFP_TEST "$sql" $map

# sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_hwxx_zzsfpxx t inner join dzdz_fpxx_zzsfp f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
# map=ID=DOUBLE,SL=DOUBLE,DJ=DOUBLE,JE=DOUBLE,SLV=DOUBLE,SE=DOUBLE,KPYF=DOUBLE
# bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_HWXX_ZZSFP "$sql" $map
