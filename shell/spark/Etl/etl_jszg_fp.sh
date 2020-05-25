#!/bin/bash
# $1 jar
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlFpPz $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlFpXs $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlFpLy $1
bash sparksubmit.sh com.aisino.bi.etl.jszg.EtlFpLyMx $1
