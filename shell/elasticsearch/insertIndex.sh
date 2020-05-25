#!/bin/bash
# $1: index
# $2: suffix :  main or spare

if [ $# -lt 1 ] ; then
    echo "[ECHO] At least one parameter is needed, the script will exit. " 
    exit

elif [ $# -eq 1 ] ; then
    suffix=""

elif [ $# -eq 2 ] ; then
    suffix="_$2"

else
    echo "[ECHO] Only two parameters most is needed, the script will exit. " 
    exit
 
fi

# get the suffix of index at the configuration 
#indexSuf=${"$1_suffix"}
#echo $indexSuf

index="$1$suffix.list"
echo ""
echo "[ECHO] Prepare to insert into index: $index"
date "+%F %T"
# setRefreshIntervalOn
bash ${ESPath}/setIndexRefreshInterval.sh $index -1s

# insert date
bash ${ESPath}/insertIndexData.sh $1 $suffix

# setRefreshIntervalOff
bash ${ESPath}/setIndexRefreshInterval.sh $index 1s

# set max num
bash ${ESPath}/setIndexMaxnum.sh $index

# delete index
bash ${ESPath}/delIndex.sh $index
echo "[ECHO] insert into index: $index finished"
date "+%F %T"
echo ""
