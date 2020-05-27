insert overwrite table es.dw_xxfp_main
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
        nvl(d4.wdbz,'') wdbz,
		nvl(substr(t.kprq,12,8),'') kpsj,
		nvl(t.skm,'') skm,
		nvl(t.bz,'') bz
from    ( select * from dw_bak.dw_fact_xxfp where datekey = ${hivevar:datekey} ) t
join    dw.dw_dm_nsr d2 on t.xf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.gf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
left join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key 
;


insert overwrite table es.dw_xxfpmx_main
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
        nvl(d4.wdbz,'') wdbz,
		nvl(substr(t.kprq,12,8),'') kpsj,
		nvl(fp.skm,'') skm,
		nvl(fp.bz,'') bz
from    ( select * from dw_bak.dw_fact_xxfpmx where datekey = ${hivevar:datekey} ) t
join    ( select xxfp_id,skm,bz from dw_bak.dw_fact_xxfp where datekey = ${hivevar:datekey} ) fp on fp.xxfp_id = t.xxfp_id
join    dw.dw_dm_nsr d2 on t.xf_nsr_key = d2.nsr_key
left join    dw.dw_dm_nsr d6 on t.gf_nsr_key = d6.nsr_key
left join    dw.dw_dim_region d4 on d6.region_key = d4.region_key
left join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
left join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
left join    dw.dw_dim_hy h on h.hy_key = d2.hy_key
;

 