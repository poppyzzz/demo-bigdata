#!/bin/bash
connect=${gp_connect}
echo "[SqoopExport] exporting to target_database: $connect"
#connect=jdbc:postgresql://192.168.24.222:25432/aisinobi
echo "====================export=start========================"$3"=========================================================================="
date "+%F %T"
#export JAVA_HOME=/usr/java/jdk1.7.0_45-cloudera
export PATH=$PATH:$JAVA_HOME/bin
if [ $# -lt 3 ]; then
  echo  "The usage is :  ./SqoopExport.sh {jarfile} {username} {tablename} {primarykey[optional]}";
else
   ###TableName=$(echo $3 | tr '[a-z]' '[A-Z]')
  TableName=$(echo $3)
   hadoop fs -rm ${export_dir}/$3/*.parquet
  bash sparksubmit.sh com.aisino.bi.util.ExportUtil $1 $2 $3

  if [ $# == 3 ]; then
    sqoop export --connect ${connect} --username $2 --password-file /user/hive/.$2 --export-dir ${export_dir}/$3 --outdir ${tmp_dir} --bindir ${tmp_dir} --input-fields-terminated-by '\001' --input-null-string '\\N' --input-null-non-string '\\N' --table $3 -m 16 -- --schema dw
  elif [ $# == 4 ]; then
    sqoop export --connect ${connect} --username $2 --password-file /user/hive/.$2 --export-dir ${export_dir}/$3 --outdir ${tmp_dir} --bindir ${tmp_dir} --input-fields-terminated-by '\001' --input-null-string '\\N' --input-null-non-string '\\N' --table $4 -m 16 -- --schema dw
   fi
fi
echo "====================export=end========================"$3"=======================================================end=================="
date "+%F %T"
