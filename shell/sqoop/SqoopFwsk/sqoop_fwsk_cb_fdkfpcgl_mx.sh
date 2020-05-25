#!/bin/bash
sql="select * from htjs.cb_fdkfpcgl_mx where bssj between to_date('$1', 'yyyy-mm-dd') and to_date('$2 23:59:59', 'yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=KPRQ=STRING,KPJH=STRING,BSYF=STRING,NSR_XZ=STRING
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh ${fwsk_username} fwsk CB_FDKFPCGL_MX_TEST "$sql" $map
