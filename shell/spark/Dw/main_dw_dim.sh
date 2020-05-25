#!/bin/bash
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimHy $1
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimSwjg $1
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimSwjgJc $1
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimRegion $1
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimFrssd $1
bash sparksubmit.sh com.aisino.bi.dw.dim.DwDimDate $1 "2000-01-01" "2030-12-31"
