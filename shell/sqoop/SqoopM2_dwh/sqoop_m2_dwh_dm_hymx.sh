#!/bin/bash
sql="select * from hx_dm_qg.DM_GY_HY h where h.xlbz='Y' and \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DM_HYMX "$sql"
