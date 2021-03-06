drop table if exists es.dw_xxfp_es_spare;
create external table if not exists es.dw_xxfp_es_spare(
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
    je decimal(16,2) ,
    se decimal(16,2) ,
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
    mldm string,
    swjglevel string,
    dkbz string,
    wdbz string,
    kpsj string,
    skm string,
    bz string
)STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'xxfp_spare.list/list','es.mapping.id' = 'id',
              'es.batch.write.retry.count'='10','es.nodes'='${hivevar:es_nodes}',
              'es.batch.size.entries' = '100000','es.batch.write.retry.wait' = '10s',
              'es.batch.write.refresh' = 'false','es.nodes.discovery' = 'true','es.nodes.client.only' = 'false'
              );

drop table if exists es.dw_xxfpmx_es_spare;
create external table if not exists es.dw_xxfpmx_es_spare(
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
    dj decimal(16,2),
    je decimal(16,2),
    sl string,
    se decimal(16,2),
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
    wdbz string,
    kpsj string,
    skm string,
    bz string
)STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'xxfpmx_spare.list/list','es.mapping.id' = 'id',
              'es.batch.write.retry.count'='10','es.nodes'='${hivevar:es_nodes}',
              'es.batch.size.entries' = '100000','es.batch.write.retry.wait' = '10s',
              'es.batch.write.refresh' = 'false','es.nodes.discovery' = 'true','es.nodes.client.only' = 'false'
              );
 
