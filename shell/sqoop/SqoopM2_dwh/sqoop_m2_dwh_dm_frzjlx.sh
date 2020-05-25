#!/bin/bash
sql="select * from hx_dm_qg.DM_GY_SFZJLX t where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DM_FRZJLX "$sql"
