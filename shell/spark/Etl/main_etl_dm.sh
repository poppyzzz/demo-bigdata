#!/bin/bash

bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlHy $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlHymx $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlHyDl $1
# bash sparksubmit.sh com.aisino.bi.etl.fwsk.EtlSwjg $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlSwjg $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlHyMl $1
