#!/bin/bash
sql="select * from hx_dm_zdy.DM_GY_SWJG t where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DM_SWJG "$sql"
