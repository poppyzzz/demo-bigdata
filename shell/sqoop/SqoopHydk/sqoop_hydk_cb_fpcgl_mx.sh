#!/bin/bash
sql="select * from htjs.cb_fpcgl_mx where bssj between to_date('$1', 'yyyy-mm-dd') and to_date('$2 23:59:59', 'yyyy-mm-dd hh24:mi:ss') and  \$CONDITIONS"
map=KPRQ=STRING
bash ${UtilPath}/SqoopJdbc_Yfwsk.sh fwsk_select fwsk CB_FPCGL_MX "$sql"
