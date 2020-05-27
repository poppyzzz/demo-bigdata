#!/bin/bash
IP=${elasticSearch_ip}
echo "elasticSearch's IP is: "$IP

#IP=192.168.24.225
curl -XDELETE $IP':9200/jxfp.list'
curl -XPUT $IP':9200/jxfp.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "skm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "wdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/jxfpmx.list'
curl -XPUT $IP':9200/jxfpmx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "gfnsrkey":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "wdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/xxfp.list'
curl -XPUT $IP':9200/xxfp.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "wdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/xxfpmx.list'
curl -XPUT $IP':9200/xxfpmx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "wdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}' 

curl -XDELETE $IP':9200/fplyd.list'
curl -XPUT $IP':9200/fplyd.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "dqdm":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/fplyd_mx.list'
curl -XPUT $IP':9200/fplyd_mx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "gfnsrkey":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "dqdm":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/fplxd.list'
curl -XPUT $IP':9200/fplxd.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "dqdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/fplxd_mx.list'
curl -XPUT $IP':9200/fplxd_mx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "dqdm":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'



curl -XDELETE $IP':9200/hwl_jx.list'
curl -XPUT $IP':9200/hwl_jx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "wp_sl":{"type":"double"},
                "fpfs":{"type":"integer","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'


curl -XDELETE $IP':9200/hwl_xx.list'
curl -XPUT $IP':9200/hwl_xx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "wp_sl":{"type":"double"},
                "fpfs":{"type":"integer","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'



curl -XDELETE $IP':9200/jxxcy_jx.list'
curl -XPUT $IP':9200/jxxcy_jx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "wp_sl":{"type":"double"},
                "fpfs":{"type":"integer","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'


curl -XDELETE $IP':9200/jxxcy_xx.list'
curl -XPUT $IP':9200/jxxcy_xx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "wp_sl":{"type":"double"},
                "fpfs":{"type":"integer","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'
 


curl -XDELETE $IP':9200/zgkpxe.list'                                            
curl -XPUT $IP':9200/zgkpxe.list?pretty' -d '                                               
{                                                   
    "mappings":{                                                    
        "list":{                                                    
            "properties":{      
                "ID":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},    
                "fplbmc":{"type":"string","index":"not_analyzed"},  
                "fpdm":{"type":"string","index":"not_analyzed"},    
                "fphm":{"type":"string","index":"not_analyzed"},    
                "kprq":{"type":"string","index":"not_analyzed"},    
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},    
                "gfnsrmc":{"type":"string","index":"not_analyzed"}, 
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},    
                "xfnsrmc":{"type":"string","index":"not_analyzed"}, 
                "je":{"type":"double"},  
                "se":{"type":"double"},  
                "fddbrmc":{"type":"string","index":"not_analyzed"}, 
                "frzjhm":{"type":"string","index":"not_analyzed"},  
                "cwlxr":{"type":"string","index":"not_analyzed"},   
                "scjydz":{"type":"string","index":"not_analyzed"},  
                "xxfpid":{"type":"string","index":"not_analyzed"},  
                "datekey":{"type":"string","index":"not_analyzed"}                         
            }                                                       
        }                                                       
    }                                                       
}'


curl -XDELETE $IP':9200/zgkpxe_mx.list'
curl -XPUT $IP':9200/zgkpxe_mx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "sl":{"type":"double"},
                "dj":{"type":"double"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/xxfpmx_zf.list'
curl -XPUT $IP':9200/xxfpmx_zf.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"}

            }
        }
    }
}'



curl -XDELETE $IP':9200/xxfp_zf.list'
curl -XPUT $IP':9200/xxfp_zf.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
		"zfsj":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/skfp.list'
curl -XPUT $IP':9200/skfp.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "rzjg":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/skfpmx.list'
curl -XPUT $IP':9200/skfpmx.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "xjswjgdm":{"type":"string","index":"not_analyzed"},
                "xjswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "xxfpid":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "rzjg":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'


 
curl -XDELETE $IP':9200/jxfp_zf.list'
curl -XPUT $IP':9200/jxfp_zf.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "je":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "skm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "zfsj":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

curl -XDELETE $IP':9200/jxfpmx_zf.list'
curl -XPUT $IP':9200/jxfpmx_zf.list?pretty' -d '
{
    "mappings":{
        "list":{
            "properties":{
                "id":{"type":"string","index":"not_analyzed"},
                "jxfpid":{"type":"string","index":"not_analyzed"},
                "fplb":{"type":"string","index":"not_analyzed"},
                "fplbmc":{"type":"string","index":"not_analyzed"},
                "fpdm":{"type":"string","index":"not_analyzed"},
                "fphm":{"type":"string","index":"not_analyzed"},
                "rzrq":{"type":"string","index":"not_analyzed"},
                "kprq":{"type":"string","index":"not_analyzed"},
                "gfnsrsbh":{"type":"string","index":"not_analyzed"},
                "gfnsrmc":{"type":"string","index":"not_analyzed"},
                "xfnsrsbh":{"type":"string","index":"not_analyzed"},
                "xfnsrmc":{"type":"string","index":"not_analyzed"},
                "wpmc":{"type":"string","index":"not_analyzed"},
                "wpxh":{"type":"string","index":"not_analyzed"},
                "wpdw":{"type":"string","index":"not_analyzed"},
                "wpsl":{"type":"string","index":"not_analyzed"},
                "dj":{"type":"double"},
                "je":{"type":"double"},
                "sl":{"type":"double"},
                "se":{"type":"double"},
                "gfswjgdm":{"type":"string","index":"not_analyzed"},
                "gfswjgmc":{"type":"string","index":"not_analyzed"},
                "gfnsrkey":{"type":"string","index":"not_analyzed"},
                "fddbrmc":{"type":"string","index":"not_analyzed"},
                "frzjhm":{"type":"string","index":"not_analyzed"},
                "cwlxr":{"type":"string","index":"not_analyzed"},
                "scjydz":{"type":"string","index":"not_analyzed"},
                "nsrmc":{"type":"string","index":"not_analyzed"},
                "nsrsbh":{"type":"string","index":"not_analyzed"},
                "sjswjgdm":{"type":"string","index":"not_analyzed"},
                "datekey":{"type":"string","index":"not_analyzed"},
                "hydm":{"type":"string","index":"not_analyzed"},
                "mldm":{"type":"string","index":"not_analyzed"},
                "swjglevel":{"type":"string","index":"not_analyzed"},
                "qdbz":{"type":"string","index":"not_analyzed"},
                "dkbz":{"type":"string","index":"not_analyzed"},
                "zfsj":{"type":"string","index":"not_analyzed"}
            }
        }
    }
}'

