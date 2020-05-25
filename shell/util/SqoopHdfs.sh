#!/bin/bash
connect=${gp_connect}
echo "[SqoopHdfs] mapping database for target_database: $connect"

if [ $# -lt 2 ]; then
  echo  "The usage is :  ./SqoopHdfs.sh {username} {tablename} {map[optional]}";
else
  ###TableName=$(echo $2 | tr '[a-z]' '[A-Z]')
  TableName=$(echo $2)
  if [ $# == 2 ]; then
    sqoop import --connect ${connect} --table ${TableName} --username $1 --password-file /user/hive/.$1 --where "1=0"  --target-dir /user/hive/warehouse/exportGreenplum/$2 --outdir ${tmp_dir} --bindir ${tmp_dir} -m 1 --as-parquetfile  -- --schema dw
  elif [ $# == 3 ]; then
    sqoop import --connect ${connect} --table ${TableName} --username $1 --password-file /user/hive/.$1 --where "1=0" --target-dir /user/hive/warehouse/exportGreenplum/$2 --outdir ${tmp_dir} --bindir ${tmp_dir} -m 1 --map-column-java $3 --as-parquetfile  -- --schema dw
  fi
fi
