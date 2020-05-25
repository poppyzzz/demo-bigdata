#!/bin/bash
sql="select  t.nsrsbh,t.nsrmc,t.swjg_dm,t.swjg_mc,t.ncpqybz from ht_ybnsr_dab t where t.ncpqybz in(1,3) and  \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc.sh ${m2_dwh_username} m2_dwh SWSK_NSR_INFO  "$sql"
