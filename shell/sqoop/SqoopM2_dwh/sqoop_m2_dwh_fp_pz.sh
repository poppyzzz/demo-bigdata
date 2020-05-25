#!/bin/bash
sql="select nvl(to_char(b.djxh),b.nsrsbh) as NSRDZDAH,a.FPZL_DM,'' as FPKJFS_DM,a.FPGPFS_DM,a.MYZGGPSL,a.MCZGGPSL,a.CPZGSL,a.DFFPZGKPXE,'' as LCPBBZ,a.YXQQ,a.YXQZ,'' as JBR,'' as FZR,'' as ICKH,'' as PZRQ,'' as CL_YWHJ_DM,'' as CLXH,'' as CLYY,'' as LRR_DM,a.LRRQ,a.XGR_DM,a.XGRQ,a.SWJG_DM,a.DFFPZGKPXE_DM,a.HDQCUUID,a.DJXH,a.YXBZ,a.WTDKBZ  from hx_fp.FP_PZHDXX a,hx_dj.dj_nsrxx b  where a.djxh=b.djxh and a.lrrq between to_date('$1','yyyy-mm-dd') and to_date('$2 23:59:59','yyyy-mm-dd hh24:mi:ss') and \$CONDITIONS"
map=MYZGGPSL=DOUBLE,MCZGGPSL=DOUBLE,CPZGSL=DOUBLE,DFFPZGKPXE=DOUBLE,YXQQ=STRING,YXQZ=STRING,LRRQ=STRING,XGRQ=STRING
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh FP_PZ "$sql" $map
