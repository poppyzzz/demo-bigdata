#!/bin/bash
#sql="select * from HX_DM_QG.DM_GY_HY h where h.zlbz='Y' and \$CONDITIONS"
sql="select a.hy_dm,a.hymc,a.mlbz,a.dlbz,a.zlbz,a.xlbz,b.sjhy_dm,a.xybz,a.yxbz from hx_dm_qg.DM_GY_HY a,hx_dm_qg.DM_GY_HY b where (a.xlbz='Y' or a.dlbz='Y') and a.sjhy_dm=b.hy_dm and \$CONDITIONS"
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh DM_HY "$sql"
