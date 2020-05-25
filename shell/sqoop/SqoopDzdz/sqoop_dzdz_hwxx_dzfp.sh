#!/bin/bash
sql="select t.* from dzdz_hwxx_dzfp t inner join dzdz_fpxx_dzfp f on t.fpdm=f.fpdm and t.fphm=f.fphm where f.kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=SL=DOUBLE,DJ=DOUBLE,JE=DOUBLE,SHUL=DOUBLE,SE=DOUBLE,KPYF=DOUBLE
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_HWXX_DZFP "$sql" $map