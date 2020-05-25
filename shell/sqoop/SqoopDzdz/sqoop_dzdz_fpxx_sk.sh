#!/bin/bash
sql="select * from dzdz_fpxx_sk t where skrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=JE=DOUBLE,SE=DOUBLE,JSHJ=DOUBLE,KPJH=DOUBLE,RZDKL_RZYF=DOUBLE
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_FPXX_SK "$sql" $map
