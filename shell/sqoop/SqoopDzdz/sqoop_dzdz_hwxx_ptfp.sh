#!/bin/bash
sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_hwxx_ptfp2017 t inner join dzdz_fpxx_ptfp2017 f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
map=ID=DOUBLE,SL=DOUBLE,DJ=DOUBLE,JE=DOUBLE,SLV=DOUBLE,SE=DOUBLE,KPYF=DOUBLE
bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_HWXX_PTFP "$sql" $map

# sql="select * from (select rownum as ROWIDSTR,t.* from dzdz_hwxx_ptfpxx t inner join dzdz_fpxx_ptfp f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss')) where \$CONDITIONS"
# map=ID=DOUBLE,SL=DOUBLE,DJ=DOUBLE,JE=DOUBLE,SLV=DOUBLE,SE=DOUBLE,KPYF=DOUBLE
# bash ${UtilPath}/SqoopJdbc_DzdzBigTable.sh DZDZ dzdz DZDZ_HWXX_PTFP "$sql" $map
