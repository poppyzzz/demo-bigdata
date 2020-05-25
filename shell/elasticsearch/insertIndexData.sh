#!/bin/bash
if [ $1 == 'jxfp' ] ; then
    sql="insert into yh_es.dw_jxfp_es$2 select * from yh_dw.dw_jxfp$2"
elif [ $1 == 'jxfpmx' ] ; then
    sql="insert into yh_es.dw_jxfpmx_es$2 select * from yh_dw.dw_jxfpmx$2"
elif [ $1 == 'xxfp' ] ; then
    sql="insert into yh_es.dw_xxfp_es$2 select * from yh_dw.dw_xxfp$2"
elif [ $1 == 'xxfpmx' ] ; then
    sql="insert into yh_es.dw_xxfpmx_es$2 select * from yh_dw.dw_xxfpmx$2"
elif [ $1 == 'fplyd' ] ; then
    sql="insert into yh_es.dw_fplyd_es$2 select * from yh_dw.dw_fplyd$2"
elif [ $1 == 'fplyd_mx' ] ; then
    sql="insert into yh_es.dw_fplyd_mx_es$2 select * from yh_dw.dw_fplyd_mx$2"
elif [ $1 == 'fplxd' ] ; then
    sql="insert into yh_es.dw_fplxd_es$2 select * from yh_dw.dw_fplxd$2"
elif [ $1 == 'fplxd_mx' ] ; then
    sql="insert into yh_es.dw_fplxd_mx_es$2 select * from yh_dw.dw_fplxd_mx$2"
elif [ $1 == 'hwl_jx' ] ; then
    sql="insert into yh_es.dw_hwl_jx_es$2 select * from yh_dw.dw_hwl_jx$2"
elif [ $1 == 'hwl_xx' ] ; then
    sql="insert into yh_es.dw_hwl_xx_es$2 select * from yh_dw.dw_hwl_xx$2"
elif [ $1 == 'jxxcy_jx' ] ; then
    sql="insert into yh_es.dw_jxxcy_jx_es$2 select * from yh_dw.dw_jxxcy_jx$2"
elif [ $1 == 'jxxcy_xx' ] ; then
    sql="insert into yh_es.dw_jxxcy_xx_es$2 select * from yh_dw.dw_jxxcy_xx$2"
elif [ $1 == 'zgkpxe' ] ; then
    sql="insert into yh_es.dw_zgkpxe_es$2 select * from yh_dw.dw_zgkpxe$2"
elif [ $1 == 'zgkpxe_mx' ] ; then
    sql="insert into yh_es.dw_zgkpxe_mx_es$2 select * from yh_dw.dw_zgkpxe_mx$2"
elif [ $1 == 'xxfp_zf' ] ; then
    sql="insert into yh_es.dw_xxfpzf_es$2 select * from yh_dw.dw_xxfp_zf$2"
elif [ $1 == 'xxfpmx_zf' ] ; then
    sql="insert into yh_es.dw_xxfpmxzf_es$2 select * from yh_dw.dw_xxfpmx_zf$2"
elif [ $1 == 'skfp' ] ; then
    sql="insert into yh_es.dw_skfp_es$2 select * from yh_dw.dw_skfp$2"
elif [ $1 == 'skfpmx' ] ; then
    sql="insert into yh_es.dw_skfpmx_es$2 select * from yh_dw.dw_skfpmx$2"
elif [ $1 == 'jxfp_zf' ] ; then
    sql="insert into yh_es.dw_jxfp_zf_es$2 select * from yh_dw.dw_jxfp_zf$2"
elif [ $1 == 'jxfpmx_zf' ] ; then
    sql="insert into yh_es.dw_jxfpmx_zf_es$2 select * from yh_dw.dw_jxfpmx_zf$2"
else 
     echo "[ECHO] Index $1 not found, the script will exit." 
     exit;
fi
    
echo "[ECHO] $sql"	 
hive -e "$sql"

