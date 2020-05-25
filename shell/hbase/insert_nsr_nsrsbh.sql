set hive.auto.convert.join=false;
insert into dw.dw_hbase_nsr
select xx.old_nsrsbh rowkey,
nsr.nsrxx nsrxx,
nsr.nsrkz nsrkz,
nsr.nsrrd nsrrd,
nsr.ckts ckts,
nsr.hy hy,
nsr.hy fppz,
nsr.swjg swjg,
nsr.region region,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') czsj 
from etl.etl_nsrxx xx join dw.dw_hbase_nsr nsr on xx.nsrsbh = nsr.rowkey where xx.old_nsrsbh is not null and xx.old_nsrsbh<>'';
set hive.auto.convert.join=true;