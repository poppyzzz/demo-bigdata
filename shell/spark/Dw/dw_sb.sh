#!/bin/bash
# $1 jar 
# $2 start_time
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactSbSbb $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactSbZzsYbnsr $1 
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactSbZzsYbnsrFb1 $1
bash sparksubmit.sh com.aisino.bi.dw.fact.DwFactSbZzsYbnsrFb2 $1
