#!/bin/bash
#$1:jar
#$2:date
#$4:path


bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlSbSbb $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlSbZzsYbnsr $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlSbZzsYbnsrFb1 $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlSbZzsYbnsrFb2 $1

