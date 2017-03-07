ls /usr/lib/spark/bin
mkdir bin
cp /usr/lib/spark/bin/load-spark-env.sh bin
mv pyspark bin/
mkdir conf
grep -v PYSPARK_PYTHON /usr/lib/spark/conf/spark-env.sh > conf/spark-env.sh

export PYSPARK_PYTHON=./memexeval2017.zip/memexeval2017/bin/python
export PYSPARK_DRIVER_PYTHON=./memexeval2017/memexeval2017/bin/python
export DEFAULT_PYTHON=./memexeval2017.zip/memexeval2017/bin/python
./memexeval2017/memexeval2017/bin/python -c "print 'hello jason'"
./bin/pyspark \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./memexeval2017.zip/memexeval2017/bin/python \
  --conf spark.executorEnv.PYSPARK_PYTHON=./memexeval2017.zip/memexeval2017/bin/python \
  --conf spark.executorEnv.DEFAULT_PYTHON=./memexeval2017.zip/memexeval2017/bin/python \
 --master yarn-client \
 --num-executors 500  --executor-memory 12g \
--archives memexeval2017.zip \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://memex/user/spark/applicationHistory \
    --conf spark.yarn.historyServer.address=memex-spark-master.xdata.data-tactics-corp.com:18080 \
    --conf spark.logConf=true \
 --py-files initExtractors2.py \
  spark_workflow.py \
  $@