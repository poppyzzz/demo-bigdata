#!/bin/bash
#$1:jar
#$2:start_time
#$3:end_time
#$4:path
DATE=$(date -d"$2" +"%Y%m")
bash sparksubmit.sh com.aisino.bi.dw.jk.DwAggJxxfxNsrcyWp $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwAggJxxfxNsrcy $1 ${DATE}
# bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjXgm $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjKjxhqdjk $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.NsrDbkjDay $1 $2 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.NsrDbkj $1 ${DATE}

#疑点信息表
bash ${TargetPath}/main_target_jk.sh
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjNsrydxxb $1

#自定义疑点
##bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjZdyyd $1

# 发票虚开
bash sparksubmit.sh com.aisino.bi.dw.agg.Wpfb $1 ${DATE}
bash SparkSql-f.sh ${SqlPath}/bak_dw_jk_table.sql datekey=${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjWpfbyc $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjWpfbtzyc $1 ${DATE}

bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjZffpyc $1 $2
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjHzfpyc $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjKpsjyc $1 ${DATE}

# 作废异常
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjZfyc $1 ${DATE}

#领用指标
##bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjLykj $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjPflyyc $1 ${DATE}

#两头在外
bash sparksubmit.sh com.aisino.bi.dw.agg.JxfxNsrZb $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.XxfxNsrZb $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjLtzw $1 ${DATE}

#省外金额突增
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjSwjeTz $1 ${DATE}

#失控发票异常
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjSkfpyc $1 ${DATE}

# 连续开票异常
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjLxkpyc $1 ${DATE}

# 工业企业预警
bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjGyqyyj $1 ${DATE}

# 备份预警模块
bash SparkSql-f.sh ${SqlPath}/bakjk.sql datekey=${DATE}

#纳税人预警聚合
bash sparksubmit.sh com.aisino.bi.dw.agg.NsrYj $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.NsrYjAll $1 ${DATE}
# 备份纳税人预警聚合
bash SparkSql-f.sh ${SqlPath}/baknsrjk.sql datekey=${DATE}

#税务机关预警聚合
bash sparksubmit.sh com.aisino.bi.dw.agg.SwjgYj $1 ${DATE}
bash sparksubmit.sh com.aisino.bi.dw.agg.SwjgYjXbqy $1 ${DATE}
#开票软件版本监控
#bash sparksubmit.sh com.aisino.bi.dw.jk.DwYjYdxxJh $1 

#纳税人新办企业
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimNsrXbqy $1 ${DATE}
