#!/bin/bash
sql="select * from DM_GY_SWRY t where \$CONDITIONS"
bash  ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DM_GY_SWRY "$sql"
