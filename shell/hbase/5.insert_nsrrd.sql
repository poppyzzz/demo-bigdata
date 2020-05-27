set hive.auto.convert.join=false;
insert into dw.dw_hbase_nsr
select nsr.rowkey rowkey,map() nsrxx,
map() nsrkz,
map('nsrzg_dm',COALESCE(rd.nsrzg_dm,''),'wspzxh',COALESCE(rd.wspzxh,''),'rdrq',COALESCE(cast(rd.rdrq as string),''),
'yxq_q',COALESCE(cast(rd.yxq_q as string),''),'yxq_z',COALESCE(cast(rd.yxq_z as string),''),'dkbz',COALESCE(rd.dkbz,''),
'zzsrdjb_dm',COALESCE(rd.zzsrdjb_dm,''),'nsr_swjg_dm',COALESCE(rd.nsr_swjg_dm,''),'swjg_dm',COALESCE(rd.swjg_dm,''),
'lrr_dm',COALESCE(rd.lrr_dm,''),'lrrq',COALESCE(cast(rd.lrrq as string),''),'xgr_dm',COALESCE(rd.xgr_dm,''),'xgrq',COALESCE(cast(rd.xgrq as string),'')) nsrrd,
map() ckts,
map() hy,
map() fppz,
map() swjg,
map() region,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') czsj
from etl.etl_nsrrd rd join dw.dw_hbase_nsr nsr on rd.nsrdzdah = nsr.nsrxx['nsrdzdah'] where (rd.nsrzg_dm ='201' or rd.nsrzg_dm='202' or rd.nsrzg_dm='203');
set hive.auto.convert.join=true;