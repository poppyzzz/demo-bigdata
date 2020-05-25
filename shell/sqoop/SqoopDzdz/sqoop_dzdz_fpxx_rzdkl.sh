#!/bin/bash
sql="select * from dzdz.dzdz_fpxx_rzdkl t where kprq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=JE=DOUBLE,SE=DOUBLE,KPRQ=STRING,RZSJ=STRING,RZYF=BIGINT,TSRQ=STRING
bash ${UtilPath}/SqoopJdbc_DzdzXt.sh fwsk_yj_zjaxn dzdz DZDZ_FPXX_RZDKL "$sql" $map
