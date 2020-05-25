set hive.auto.convert.join=false;
insert into dw.dw_hbase_nsr
select nsr.rowkey rowkey,
map() nsrxx,
map() nsrkz,
map() nsrrd,
map() ckts,
map('hy_key',COALESCE(h.hy_key,''),'hy_dm',COALESCE(h.hy_dm,''),'hy_mc',COALESCE(h.hy_mc,''),
'hymx_dm',COALESCE(h.hymx_dm,''),'hymx_mc',COALESCE(h.hymx_mc,''),'yxbz',COALESCE(h.yxbz,''),
'hydl_dm',COALESCE(h.hydl_dm,''),'hydl_mc',COALESCE(h.hydl_mc,''),'hyml_dm',COALESCE(h.hyml_dm,''),
'hyml_mc',COALESCE(h.hyml_mc,''),'mldm',COALESCE(h.mldm,''),'mlmc',COALESCE(h.mlmc,'')) hy,
map() fppz,
map() swjg,
map() region,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') czsj
from dw.dw_dim_hy h,dw.dw_hbase_nsr nsr where h.hy_key = '9999' and nsr.nsrxx['hy_dm'] is null;
set hive.auto.convert.join=true;