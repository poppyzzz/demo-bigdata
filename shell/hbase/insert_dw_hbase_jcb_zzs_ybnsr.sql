insert into dw.dw_hbase_jcb_zzs_ybnsr
select concat(w.nsr_key,w.date_key) rowkey,
map() jcb_zzs_ybnsr,
map() nsrxx,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') czsj
from dw.dw_yj_qysbyc  w