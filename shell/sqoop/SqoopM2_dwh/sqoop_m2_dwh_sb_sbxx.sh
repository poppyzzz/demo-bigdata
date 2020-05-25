#!/bin/bash
sql="select nsr.nsrsbh,sbxx.sbuuid,to_char(sbxx.pzxh) as pzxh,sbxx.ynse,sbxx.yjse,sbxx.sl_1,sbxx.jmse,sbxx.ybtse,trim(sbxx.zfbz_1) zfbz_1,sbxx.hy_dm,sbxx.lrrq from sb_sbxx sbxx, dj_nsrxx nsr where sbxx.djxh = nsr.djxh and trim(sbxx.zfbz_1)='N' and  \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc.sh ${m2_dwh_username} m2_dwh SB_SBXX "$sql"
