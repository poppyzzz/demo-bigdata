insert overwrite table yh_dw.dw_jxfp_zf
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
        nvl(zffp.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d2.nsrmc,'') nsrmc,
        nvl(d2.nsrsbh,'') nsrsbh,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.dkbz,'') dkbz,
        nvl(t.zfsj,'') zfsj
from    ( select zf.date_key,zf.fpdm,zf.fphm 
          from ( select * from dw_bak.dw_fact_zffp where date_key=${hivevar:datekey}) zf 
          join ( select * from dw_bak.dw_fact_rz_fpdkl_mx where date_key=${hivevar:datekey})  rz 
          on zf.fpdm = rz.fpdm and zf.fphm = rz.fphm 
          where zf.zfrq >= rz.rz_sj ) zffp 
join    ( select * from  dw_bak.dw_fact_jxfp_zf where date_key=${hivevar:datekey}) t on t.fpdm = zffp.fpdm and t.fphm = zffp.fphm
join    dw.dw_dm_nsr d2 on t.gf_nsr_key = d2.nsr_key
join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
join    dw.dw_dim_hy h on h.hy_key = d2.hy_key
;



insert overwrite table yh_dw.dw_jxfpmx_zf
select  concat(nvl(t.jxfpmx_key,''),"x",nvl(t.wp_mc,''),"x",nvl(t.wp_xh,''),"x",nvl(t.wp_dw,''),"x",nvl(d.sjswjg_dm,'')) id,
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
        nvl(zffp.date_key,'') datekey,
        nvl(h.hy_dm,'') hydm,
        nvl(h.mldm,'') mldm,
        nvl(d.swjglevel,'') swjglevel,
        nvl(t.qdbz,'') qdbz,
        nvl(t.dkbz,'') dkbz,
        nvl(t.zfsj,'') zfsj
from    ( select zf.date_key,zf.fpdm,zf.fphm 
          from ( select * from dw_bak.dw_fact_zffp where date_key=${hivevar:datekey}) zf 
          join ( select * from dw_bak.dw_fact_rz_fpdkl_mx where date_key=${hivevar:datekey})  rz 
          on zf.fpdm = rz.fpdm and zf.fphm = rz.fphm 
          where zf.zfrq >= rz.rz_sj 
          ) zffp 
join    ( select * from  dw_bak.dw_fact_jxfpmx_zf where date_key=${hivevar:datekey}) t on t.fpdm = zffp.fpdm and t.fphm = zffp.fphm
join    dw.dw_dm_nsr d2 on t.gf_nsr_key = d2.nsr_key
join    dw.dw_dim_swjg_jc d on d.xjswjg_key = d2.swjg_key
join    dw.dw_fplb d3 on t.fp_lb = d3.fp_lb
join    dw.dw_dim_hy h on h.hy_key = d2.hy_key
;
