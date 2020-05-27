set hive.auto.convert.join=false;
insert into dw.dw_hbase_nsr
select fp.xf_nsrsbh rowkey,map('nsrsbh',COALESCE(fp.xf_nsrsbh,''),'nsrmc',COALESCE(fp.xf_nsrmc,'')) nsrxx,
map() nsrkz,
map('nsrzg_dm','998') nsrrd,
map() ckts,
map() hy,
map() fppz,
map('swjg_key',COALESCE(fp.xf_swjg_dm,''),'region_key',COALESCE(s.region_key,''),'swzj_dm',COALESCE(s.swzj_dm,''),
'swzj_mc',COALESCE(s.swzj_mc,''),'sjgs_dm',COALESCE(s.sjgs_dm,''),'sjgs_mc',COALESCE(s.sjgs_mc,''),
'sjgs_jc',COALESCE(s.sjgs_jc,''),'dsjgs_dm',COALESCE(s.dsjgs_dm,''),'dsjgs_mc',COALESCE(s.dsjgs_mc,''),
'dsjgs_jc',COALESCE(s.dsjgs_jc,''),'xqjgs_dm',COALESCE(s.xqjgs_dm,''),'xqjgs_mc',COALESCE(s.xqjgs_mc,''),
'xqjgs_jc',COALESCE(s.xqjgs_jc,''),'xzjgs_dm',COALESCE(s.xzjgs_dm,''),'xzjgs_mc',COALESCE(s.xzjgs_mc,''),
'xzjgs_jc',COALESCE(s.xzjgs_jc,''),'zjgs_dm',COALESCE(s.zjgs_dm,''),'zjgs_mc',COALESCE(s.zjgs_mc,''),
'zjgs_jc',COALESCE(s.zjgs_jc,''),'by1_dm',COALESCE(s.by1_dm,''),'by1_mc',COALESCE(s.by1_mc,''),
'by1_jc',COALESCE(s.by1_jc,''),'by2_dm',COALESCE(s.by2_dm,''),'by2_mc',COALESCE(s.by2_mc,''),
'by2_jc',COALESCE(s.by2_jc,''),'swjg_dm',COALESCE(s.swjg_dm,''),'swjg_mc',COALESCE(s.swjg_mc,''),
'swjg_jc',COALESCE(s.swjg_jc,''),'czrq',COALESCE(cast(s.czrq as string),'')) swjg,
map() region,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') czsj 
from etl.etl_jxfp fp left join dw.dw_dim_swjg s on fp.xf_swjg_dm = s.swjg_key 
left join dw.dw_hbase_nsr nsr on fp.xf_nsrsbh = nsr.rowkey where nsr.rowkey is null 
--or nsr.region is null or nsr.region['region_key'] is null or nsr.region['region_key']='' or nsr.region['region_key']='X999'
;
set hive.auto.convert.join=true;