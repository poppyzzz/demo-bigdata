#!/bin/bash
sql="select * from dzdz_da_ybnsr_zc where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_DA_YBNSR_ZC "$sql" 
