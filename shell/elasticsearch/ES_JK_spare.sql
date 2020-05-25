insert overwrite table yh_dw.dw_jxfp_spare
select
        concat(nvl(t.jxfp_id,''),"x",nvl(d.sjswjg_dm,''),'') id,
        nvl(t.fp_lb,'') fplb,
        nvl(d3.fp_lb_mc,'') fplbmc,
        nvl(t.fpdm,'') fpdm,
        nvl(t.fphm,'') fphm,
        nvl(t.rzrq,'') rzrq,
        nvl(t.kprq,'') kprq,
        nvl(t.gf_nsrsbh,'') gfnsrsbh,
        nvl(t.gf_nsrmc,'') gfnsrmc,
        nvl(t.xf_nsrsbh,'') xfnsrsbh,
        nvl(t.xf_nsrmc,'') xfnsrmc,
        nvl(t.je,'') je,
        nvl(t.se,'') se,
        nvl(d.xjswjg_dm,'') gfswjgdm,
        nvl(t.skm,'') skm,
        nvl(d.xjswjg_mc,'') gfswjgmc,
        nvl(d2.fddbr_mc,'') fddbrmc,
        nvl(d2.frzjhm,'') frzjhm,
        nvl(d2.cwlxr,'') cwlxr,
        nvl(d2.scjydz,'') scjydz,
        nvl(t.jxfp_id,'') jxfpid,
        nvl(d.sjswjg_dm,'') sjswjgdm,
        nvl(t.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d2.nsrmc,'') nsrmc,
        nvl(d2.nsrsbh,'') nsrsbh,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.dkbz,'') dkbz,
        nvl(d4.wdbz,'') wdbz
from    ( select * from dw_bak.dw_fact_jxfp where datekey = ${hivevar:datekey} ) t
left join    dw.dw_dim_swjg_jc d on t.gf_swjg_key = d.xjswjg_key
join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
join    dw.dw_dm_nsr d2 on t.gf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.xf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key 
;


insert overwrite table yh_dw.dw_jxfpmx_spare
select  
		concat(nvl(t.jxfpmx_key,''),"x",nvl(hash(t.wp_mc),''),"x",nvl(hash(t.wp_xh),''),"x",nvl(t.wp_dw,''),"x",nvl(d.sjswjg_dm,'')) id,
        nvl(t.jxfp_id,'') jxfpid,
        nvl(t.fp_lb,'') fplb,
        nvl(d3.fp_lb_mc,'') fplbmc,
        nvl(t.fpdm,'') fpdm,
        nvl(t.fphm,'') fphm,
        nvl(t.rzrq,'') rzrq,
        nvl(t.kprq,'') kprq,
        nvl(t.gf_nsrsbh,'') gfnsrsbh,
        nvl(t.gf_nsrmc,'') gfnsrmc,
        nvl(t.xf_nsrsbh,'') xfnsrsbh,
        nvl(t.xf_nsrmc,'') xfnsrmc,
        nvl(t.wp_mc,'') wpmc,
        nvl(t.wp_xh,'') wpxh,
        nvl(t.wp_dw,'') wpdw,
        nvl(t.wp_sl,'') wpsl,
        nvl(t.dj,'') dj,
        nvl(t.je,'') je,
        nvl(t.sl,'') sl,
        nvl(t.se,'') se,
        nvl(d.xjswjg_dm,'') gfswjgdm,
        nvl(d.xjswjg_mc,'') gfswjgmc,
        nvl(t.gf_nsr_key,'') gfnsrkey,
        nvl(d2.fddbr_mc,'') fddbrmc,
        nvl(d2.frzjhm,'') frzjhm,
        nvl(d2.cwlxr,'') cwlxr,
        nvl(d2.scjydz,'') scjydz,
        nvl(d2.nsrmc,'') nsrmc,
        nvl(d2.nsrsbh,'') nsrsbh,
        nvl(d.sjswjg_dm,'') sjswjgdm,
        nvl(t.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.qdbz,'') qdbz,
        nvl(t.dkbz,'') dkbz,
        nvl(d4.wdbz,'') wdbz
from    ( select * from dw_bak.dw_fact_jxfpmx where datekey = ${hivevar:datekey} ) t
join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
join    dw.dw_dm_nsr d2 on t.gf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.xf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key
;




insert overwrite table yh_dw.dw_xxfp_spare
select
        concat(nvl(t.xxfp_id,''),"x",nvl(d.sjswjg_dm,'')) id,
        nvl(t.xxfp_id,'') xxfpid,
        nvl(t.fp_lb,'') fplb,
        nvl(d3.fp_lb_mc,'') fplbmc,
        nvl(t.fpdm,'') fpdm,
        nvl(t.fphm,'') fphm,
        nvl(t.kprq,'') kprq,
        nvl(t.gf_nsrsbh,'') gfnsrsbh,
        nvl(t.gf_nsrmc,'') gfnsrmc,
        nvl(t.xf_nsrsbh,'') xfnsrsbh,
        nvl(t.xf_nsrmc,'') xfnsrmc,
        nvl(t.je,'') je,
        nvl(t.se,'') se,
        nvl(d.xjswjg_dm,'') xjswjgdm,
        nvl(d.xjswjg_mc,'') xjswjgmc,
        nvl(d2.fddbr_mc,'') fddbrmc,
        nvl(d2.frzjhm,'') frzjhm,
        nvl(d2.cwlxr,'') cwlxr,
        nvl(d2.scjydz,'') scjydz,
        nvl(d2.nsrsbh,'') nsrsbh,
        nvl(d.sjswjg_dm,'') sjswjgdm,
        nvl(t.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.dkbz,'') dkbz,
        nvl(d4.wdbz,'') wdbz
from    ( select * from dw_bak.dw_fact_xxfp where datekey = ${hivevar:datekey} ) t
join    dw.dw_dm_nsr d2 on t.xf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.gf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
left join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key 
;


insert overwrite table yh_dw.dw_xxfpmx_spare
select
        concat(nvl(t.xxfpmx_key,''),"x",nvl(hash(t.wp_mc),''),"x",nvl(hash(t.wp_xh),''),"x",nvl(t.wp_dw,''),"x",nvl(d.sjswjg_dm,'')) id,
        nvl(t.fp_lb,'') fplb,
        nvl(d3.fp_lb_mc,'') fplbmc,
        nvl(t.fpdm,'') fpdm,
        nvl(t.fphm,'') fphm,
        nvl(t.kprq,'') kprq,
        nvl(t.gf_nsrsbh,'') gfnsrsbh,
        nvl(t.gf_nsrmc,'') gfnsrmc,
        nvl(t.xf_nsrsbh,'') xfnsrsbh,
        nvl(t.xf_nsrmc,'') xfnsrmc,
        nvl(t.wp_mc,'') wpmc,
        nvl(t.wp_xh,'') wpxh,
        nvl(t.wp_dw,'') wpdw,
        nvl(t.wp_sl,'') wpsl,
        nvl(t.dj,'') dj,
        nvl(t.je,'') je,
        nvl(t.sl,'') sl,
        nvl(t.se,'') se,
        nvl(d.xjswjg_dm,'') xjswjgdm,
        nvl(d.xjswjg_mc,'') xjswjgmc,
        nvl(d2.fddbr_mc,'') fddbrmc,
        nvl(d2.frzjhm,'') frzjhm,
        nvl(d2.cwlxr,'') cwlxr,
        nvl(d2.scjydz,'') scjydz,
        nvl(t.xxfp_id,'') xxfpid,
        nvl(d2.nsrsbh,'') nsrsbh,
        nvl(d2.nsrmc,'') nsrmc,
        nvl(d.sjswjg_dm,'') sjswjgdm,
        nvl(t.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.qdbz,'') qdbz,
        nvl(t.dkbz,'') dkbz,
        nvl(d4.wdbz,'') wdbz
from    ( select * from dw_bak.dw_fact_xxfpmx where datekey = ${hivevar:datekey} ) t
join    dw.dw_dm_nsr d2 on t.xf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.gf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
left join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key
;

 

insert overwrite table yh_dw.dw_hwl_jx_spare
select
        concat(nvl(dw.nsrsbh,''),'x',nvl(t.date_key,''),'x',nvl(t.fp_lb,''),'x',nvl(t.wp_mc,''),'x',nvl(t.wp_dw,'')) id ,
        nvl(t.wp_mc,'') wpmc,
        nvl(round(sum(t.je), 2),'') je,
        nvl(round(sum(t.se), 2),'') se,
        nvl(round(sum(t.wp_sl), 2),'') wp_sl,
        nvl(sum(t.fpfs),'') fpfs,
        nvl(t.fp_lb,'') fplb,
        nvl(max(d.fp_lb_mc),'') fplbmc,
        nvl(t.wp_dw,'') wpdw,
        nvl(dw.nsrsbh,'') nsrsbh,
        nvl(t.date_key,'') datekey
from    ( select * from dw_bak.dw_agg_jxfx_wp where date_key = ${hivevar:datekey} ) t
join    dw.dw_fplb d on t.fp_lb = d.fp_lb
join    dw.dw_dm_nsr dw on t.gf_nsr_key = dw.nsr_key
group by t.wp_mc, t.fp_lb, t.wp_dw, dw.nsrsbh, t.date_key
;

insert overwrite table yh_dw.dw_hwl_xx_spare
select
        concat(nvl(dw.nsrsbh,''),'x',nvl(t.date_key,''),'x',nvl(t.fp_lb,''),'x',nvl(t.wp_mc,''),'x',nvl(t.wp_dw,'')) id ,
        nvl(t.wp_mc,'') wpmc,
        nvl(round(sum(t.je), 2),'') je,
        nvl(round(sum(t.se), 2),'') se,
        nvl(round(sum(t.wp_sl), 2),'') wp_sl,
        nvl(sum(t.fpfs),'') fpfs,
        nvl(t.fp_lb,'') fplb,
        nvl(max(d.fp_lb_mc),'') fplbmc,
        nvl(t.wp_dw,'') wpdw,
        nvl(dw.nsrsbh,'') nsrsbh,
        nvl(t.date_key,'') datekey
from    ( select * from dw_bak.dw_agg_xxfx_wp where date_key = ${hivevar:datekey} ) t
join    dw.dw_fplb d on t.fp_lb = d.fp_lb
join    dw.dw_dm_nsr dw on t.xf_nsr_key = dw.nsr_key
group by t.wp_mc, t.fp_lb, t.wp_dw, dw.nsrsbh, t.date_key
;

