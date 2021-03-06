create table if not exists es.dw_jxfp (
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    rzrq string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    je string,
    se string,
    gfswjgdm string,
    skm string,
    gfswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    jxfpid string,
    sjswjgdm string,
    datekey string,
    hydm string,
    mldm string,
    nsrmc string,
    nsrsbh string,
    swjglevel string,
    dkbz string,
    wdbz string
);

create table if not exists es.dw_jxfpmx(
    row_id bigint,
    id string ,
    jxfpid string,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    rzrq string ,
    kprq string ,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    wpmc string ,
    wpxh string ,
    wpdw string ,
    wpsl string ,
    dj string ,
    je string ,
    sl string ,
    se string ,
    gfswjgdm string ,
    gfswjgmc string ,
    gfnsrkey string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    nsrmc string ,
    nsrsbh string,
    sjswjgdm string ,
    datekey string ,
    hydm string ,
    mldm string,
    swjglevel string,
    qdbz string,
    dkbz string,
    wdbz string
);

create table if not exists es.dw_xxfp
(
    id string ,
    xxfpid string,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    kprq string ,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    je string ,
    se string ,
    xjswjgdm string ,
    xjswjgmc string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    nsrsbh string ,
    sjswjgdm string ,
    datekey string ,
    hydm string ,
    mldm string ,
    swjglevel string,
    dkbz string,
    wdbz string
);

create table if not exists es.dw_xxfpmx(
    row_id bigint,
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    dj string,
    je string,
    sl string,
    se string,
    xjswjgdm string,
    xjswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    nsrsbh string,
    nsrmc  string,
    sjswjgdm string,
    datekey string,
    hydm  string,
    mldm string,
    swjglevel string,
    qdbz string,
    dkbz string,
    wdbz string
);

create table if not exists es.dw_fplyd(
    id string ,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    rzrq string ,
    kprq string ,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    je string ,
    se string ,
    gfswjgdm string ,
    gfswjgmc string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    jxfpid string ,
    sjswjgdm string ,
    datekey string ,
    hydm string,
    mldm string,
    dqdm string
);

create table if not exists es.dw_fplyd_mx(
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    rzrq string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    dj string,
    je string,
    sl string,
    se string,
    gfswjgdm string,
    gfswjgmc string,
    gfnsrkey string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    sjswjgdm string,
    datekey string,
    jxfpid string,
    hydm string,
    mldm string,
    dqdm string,
    qdbz string
);


create table if not exists es.dw_fplxd (
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    je string,
    se string,
    xjswjgdm string,
    xjswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    sjswjgdm string,
    dqdm string,
    datekey string,
    hydm string,
    mldm string
);

create table if not exists es.dw_fplxd_mx(
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    dj string,
    je string,
    sl string,
    se string,
    xjswjgdm string,
    xjswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    sjswjgdm string,
    datekey string,
    dqdm string,
    hydm string,
    mldm string,
    qdbz string
);


create table if not exists es.dw_hwl_jx(
    id string,
    wpmc string,
    je string,
    se string,
    wp_sl string,
    fpfs string,
    fplb string,
    fplbmc string,
    wpdw string,
    nsrsbh string,
    datekey string
);

create table if not exists es.dw_hwl_xx(
    id string,
    wpmc string,
    je string,
    se string,
    wp_sl string,
    fpfs string,
    fplb string,
    fplbmc string,
    wpdw string,
    nsrsbh string,
    datekey string
);

create table if not exists es.dw_jxxcy_jx(
    id string,
    wpmc string,
    je string,
    se string,
    wp_sl string,
    fpfs string,
    fplb string,
    fplbmc string,
    wpdw string,
    nsrsbh string,
    datekey string
);

create table if not exists es.dw_jxxcy_xx(
    id string,
    wpmc string,
    je string,
    se string,
    wp_sl string,
    fpfs string,
    fplb string,
    fplbmc string,
    wpdw string,
    nsrsbh string,
    datekey string
);



create table if not exists es.dw_zgkpxe(
    ID string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    je string,
    se string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    datekey  string       
);

create table if not exists es.dw_zgkpxe_mx(
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    je string,
    se string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    sl string,
    dj string,
    xxfpid string,
    datekey string,
    qdbz string
);


create table if not exists es.dw_xxfp_zf
(
    id string ,
    xxfpid string,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    kprq string ,
    zfsj string,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    je string ,
    se string ,
    xjswjgdm string ,
    xjswjgmc string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    nsrsbh string ,
    sjswjgdm string ,
    datekey string ,
    hydm string ,
    mldm string ,
    swjglevel string
);

create table if not exists es.dw_xxfpmx_zf(
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    zfsj string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    dj string,
    je string,
    sl string,
    se string,
    xjswjgdm string,
    xjswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    nsrsbh string,
    nsrmc  string,
    sjswjgdm string,
    datekey string,
    hydm  string,
    mldm string,
    swjglevel string,
    qdbz string

);

create table if not exists es.dw_skfp
(
    id string ,
    xxfpid string,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    kprq string ,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    je string ,
    se string ,
    xjswjgdm string ,
    xjswjgmc string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    nsrsbh string ,
    sjswjgdm string ,
    datekey string ,
    hydm string ,
    mldm string ,
    swjglevel string,
    dkbz string,
    rzjg string
);

create table if not exists es.dw_skfpmx(
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    wpmc string,
    wpxh string,
    wpdw string,
    wpsl string,
    dj string,
    je string,
    sl string,
    se string,
    xjswjgdm string,
    xjswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    xxfpid string,
    nsrsbh string,
    nsrmc  string,
    sjswjgdm string,
    datekey string,
    hydm  string,
    mldm string,
    swjglevel string,
    qdbz string,
    dkbz string,
    rzjg string
);


create table if not exists es.dw_jxfp_zf (
    id string,
    fplb string,
    fplbmc string,
    fpdm string,
    fphm string,
    rzrq string,
    kprq string,
    gfnsrsbh string,
    gfnsrmc string,
    xfnsrsbh string,
    xfnsrmc string,
    je string,
    se string,
    gfswjgdm string,
    skm string,
    gfswjgmc string,
    fddbrmc string,
    frzjhm string,
    cwlxr string,
    scjydz string,
    jxfpid string,
    sjswjgdm string,
    datekey string,
    hydm string,
    mldm string,
    nsrmc string,
    nsrsbh string,
    swjglevel string,
    dkbz string,
    zfsj string
);


create table if not exists es.dw_jxfpmx_zf(
    id string ,
    jxfpid string,
    fplb string ,
    fplbmc string ,
    fpdm string ,
    fphm string ,
    rzrq string ,
    kprq string ,
    gfnsrsbh string ,
    gfnsrmc string ,
    xfnsrsbh string ,
    xfnsrmc string ,
    wpmc string ,
    wpxh string ,
    wpdw string ,
    wpsl string ,
    dj string ,
    je string ,
    sl string ,
    se string ,
    gfswjgdm string ,
    gfswjgmc string ,
    gfnsrkey string ,
    fddbrmc string ,
    frzjhm string ,
    cwlxr string ,
    scjydz string ,
    nsrmc string ,
    nsrsbh string,
    sjswjgdm string ,
    datekey string ,
    hydm string ,
    mldm string,
    swjglevel string,
    qdbz string,
    dkbz string,
    zfsj string
);
