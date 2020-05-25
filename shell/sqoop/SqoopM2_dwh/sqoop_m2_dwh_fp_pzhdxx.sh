#!/bin/bash
sql="select hdqcuuid,to_char(djxh) as djxh,fpzl_dm,dffpzgkpxe_dm,myzggpsl,mczggpsl,cpzgsl,fpgpfs_dm,yxqq,yxqz,yxbz,swjg_dm,dffpzgkpxe,wtdkbz,lrr_dm,lrrq,xgr_dm,xgrq,sjgsdq,sjtb_sj,lxkpsx,lxkpljxe from fp_pzhdxx t where \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh FP_PZHDXX "$sql"

