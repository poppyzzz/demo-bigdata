#!/bin/bash
#$1:jar
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmFactJxfpmx $1
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmFactXxfpmx $1
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmJxfxNsr $1
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmXxfxNsr $1
bash sparksubmit.sh com.aisino.bi.dw.dm.DwDmSwjgJc $1


