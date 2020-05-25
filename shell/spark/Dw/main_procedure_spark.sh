#!/bin/bash
#$1:jar
#$2:end_time
DATE=$(date -d"$2" +"%Y%m")
# bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxLydHySl $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxLydZb $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxNsrSl $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxSwjgHy $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxSwjgZb $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxLydHySl $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxLydZb $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxNsrSl $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxSwjgHy $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxSwjgZb $1 ${DATE}

bash ${DelPath}/delProcedureTable.sh $1 ${DATE}
bash ${ExportPath}/main_export_external_procedure.sh $1

# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_jxfx_lyd_hy_sl
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_jxfx_lyd_zb
#bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_jxfx_nsr_sl
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_jxfx_swjg_hy
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_jxfx_swjg_zb
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_xxfx_lyd_hy_sl
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_xxfx_lyd_zb
#bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_xxfx_nsr_sl
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_xxfx_swjg_hy
# bash ${UtilPath}/SqoopExport.sh $1 dw dw_agg_xxfx_swjg_zb
