#!/bin/bash
#sql="select to_char(ybnsrfb.pzxh) as pzxh,ybnsrfb.sbuuid,ybnsrfb.ewbhxh,ybnsrfb.fs,ybnsrfb.je,ybnsrfb.se,ybnsrfb.lrrq from sb_zzs_ybnsr_fb2_bqjxsemx ybnsrfb where  ybnsrfb.ewbhxh = 6 and  \$CONDITIONS"
#sql="select SB.NSRSBH, SB.NSRMC, SB.SKSSQQ, SB.SKSSQZ, MX.EWBHXH,  MX.HMC, MX.FS, MX.JE, MX.SE from HX_SB.SB_SBB SB LEFT JOIN HX_SB.SB_ZZS_YBNSR_FB2_BQJXSEMX MX ON MX.SBUUID = SB.SBUUID WHERE SB.YZPZZL_DM = 'BDA0610606' AND SB.SKSSQQ = to_date('$1','yyyy-MM-dd') AND SB.SKSSQZ = to_date('$2','yyyy-MM-dd') AND SB.ZFBZ_1 = 'N' AND SB.GZLX_DM_1 IN ('1', '5','4') AND MX.EWBHXH IN ('5', '6', '7', '8', '11') and \$CONDITIONS"
#sql="select SB.NSRSBH, SB.NSRMC, SB.SKSSQQ, SB.SKSSQZ, MX.EWBHXH,  MX.HMC, MX.FS, MX.JE, MX.SE from SB_SBB SB LEFT JOIN SB_ZZS_YBNSR_FB2_BQJXSEMX MX ON MX.SBUUID = SB.SBUUID WHERE SB.YZPZZL_DM = 'BDA0610606' AND SB.SKSSQQ = to_date('$1','yyyy-MM-dd') AND SB.SKSSQZ = to_date('$2','yyyy-MM-dd') AND SB.ZFBZ_1 = 'N' AND SB.GZLX_DM_1 IN ('1', '5','4') AND MX.EWBHXH IN ('5', '6', '7', '8', '11') and \$CONDITIONS"
sql="select SB.NSRSBH, SB.NSRMC, SB.SKSSQQ, SB.SKSSQZ, MX.EWBHXH,  MX.HMC, MX.FS, MX.JE, MX.SE from HX_SB.SB_SBB SB LEFT JOIN HX_SB.SB_ZZS_YBNSR_FB2_BQJXSEMX MX ON MX.SBUUID = SB.SBUUID WHERE SB.YZPZZL_DM = 'BDA0610606' AND SB.SKSSQQ = to_date('$1','yyyy-MM-dd') AND SB.ZFBZ_1 = 'N' AND SB.GZLX_DM_1 IN ('1', '5','4') AND MX.EWBHXH IN ('5', '6', '7', '8', '11') and \$CONDITIONS"

map=SKSSQQ=STRING,SKSSQZ=STRING,FS=BIGINT,JE=DOUBLE,SE=DOUBLE
bash ${UtilPath}/SqoopJdbc_M2_dwh.sh ${m2_dwh_username} m2_dwh SB_ZZS_YBNSR_FB2 "$sql" $map
