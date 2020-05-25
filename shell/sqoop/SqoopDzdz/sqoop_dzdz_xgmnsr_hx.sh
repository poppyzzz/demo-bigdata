#!/bin/bash
sql="select * from dzdz_da_xgmnsr_hx where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_Dzdz.sh DZDZ dzdz DZDZ_DA_XGMNSR_HX "$sql" 
