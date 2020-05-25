#!/bin/bash

sql="select t.* from dzdz_hwxx_hyzp2017 t inner join dzdz_fpxx_hyzp2017 f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=XH=DOUBLE,JE=DOUBLE,KPYF=DOUBLE
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_HWXX_HYZP "$sql" $map 



# sql="select t.* from dzdz_hwxx_hyzpxx t inner join dzdz_fpxx_hyzp f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
# map=XH=DOUBLE,JE=DOUBLE,KPYF=DOUBLE
# bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_HWXX_HYZP "$sql" $map 

