#!/bin/bash

echo "###########################Getting Data from Source and Loading it to HDFS#################################"

./extract.sh >> /dev/null 2>&1

sleep 3

echo "###############################Transformation and Loading Data to HIVE#####################################"

spark-submit --conf spark.sql.catalogImplementation=hive transform.py >> /dev/null 2>&1

sleep 3

echo "##############################Creating Vizualization########################################################"

spark-submit --conf spark.sql.catalogImplementation=hive viz.py >> /dev/null 2>&1

sleep 3

echo "####################################Copying Viz to dev.cs.smu.ca/~j_mahajan/covid/###########################"

scp index.html j_mahajan@dev.cs.smu.ca:public_html/covid/ >> /dev/null 2>&1

sleep 3

echo "--->>>Viz is ready to view at dev.cs.smu.ca/~j_mahajan/covid/ <<<---"
