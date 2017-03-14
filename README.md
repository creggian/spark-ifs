## Feature selection in high-dimensional dataset using MapReduce

This is a scala library for Apache Spark for feature selection algorithms in MapReduce. This is the source code of the paper *Feature selection in high-dimensional dataset using MapReduce* (under revision)

### Content

* **data**: there are two example datasets to test the library locally or in the cluster.
  * ''mrmr.50x20.cw.c0.x1_8.csv'' has 50 rows and 21 columns. Data is encoded with the traditional layout. The first column, x0, which represent the class is calculated with the following formula: ( ((x1 & x2) | (x3 & x4)) & ((x5 & x6) | (x7 & x8)) ), as similarly done in the CorrAL dataset.
  * ''mrmr.50x20.rw.x0_7.csv'' has 50 rows and 21 columns, where the first column is the column name (unique among all the other names). ''mrmr.50x20.rw.x0_7.class.csv'' provides the class vector.
* **src** includes all the sources.
* **target** includes, among other file, the *mrmr_2.10-1.0.jar* located in ''target/scala-2.10'' folder. It is the executable built using scala 2.10 and apache spark 1.5.0.
* **simple.sbt** describes the built settings.
* **compile<span></span>.sh** is a simple script use to run the project compilation in one step.

### Example

There is an example dataset in the *data* folder. You need first to load the data in HDFS and then you can run the code below (eventually, update the paths to all files). 

The code is ready to perform locally, change *--master* option value from ''local[*]'' to ''yarn-cluster'' to run the algorithm in the distributed environment.

#### Conventional encoding

##### CSV

```
INPUTFILE="data/mrmr.50x20.cw.c0.x1_8.csv"                   # input path
TYPE="column-wise"                                           # traditional encoding
NFS="5"                                                      # number of feature to select
FORMAT="csv"                                                 # CSV input format
LABELIDX="0"                                                 # index of the label
SCORECLASS="creggian.mrmr.feature.common.InstanceWiseMRMR"   # score class name

MRMRINPUT="$INPUTFILE $TYPE $NFS $FORMAT $LABELIDX $SCORECLASS"

$SPARK_HOME/bin/spark-submit \
    --class creggian.mrmr.Run \
    --master local[*] \
    --num-executors 1 \
    --executor-memory 4G \
    --driver-memory 4G \
    target/scala-2.10/mrmr_2.10-1.0.jar \
    $MRMRINPUT \
    2> /dev/null
```

##### LibSVM

```
INPUTFILE="data/mrmr.50x20.cw.c0.x1_8.libsvm"                # input path
TYPE="column-wise"                                           # traditional encoding
NFS="5"                                                      # number of feature to select
FORMAT="libsvm"                                              # LibSVM input format
LABELIDX="0"                                                 # index of the label
SCORECLASS="creggian.mrmr.feature.common.InstanceWiseMRMR"   # score class name

MRMRINPUT="$INPUTFILE $TYPE $NFS $FORMAT $LABELIDX $SCORECLASS"

$SPARK_HOME/bin/spark-submit \
    --class creggian.mrmr.Run \
    --master local[*] \
    --num-executors 1 \
    --executor-memory 4G \
    --driver-memory 4G \
    --conf "spark.default.parallelism=10" \
    target/scala-2.10/mrmr_2.10-1.0.jar \
    $MRMRINPUT \
    2> errors.log
```


#### Alternative encoding

##### CSV

```
INPUTFILE="data/mrmr.50x20.rw.x0_7.csv"                      # input path
TYPE="row-wise"                                              # alternative encoding
NFS="5"                                                      # number of feature to select
FORMAT="csv"                                                 # CSV input format
LABELIDX="0"                                                 # index of the label
SCORECLASS="creggian.mrmr.feature.common.FeatureWiseMRMR"    # score class name
LABELFILE="data/mrmr.50x20.rw.x0_7.class.csv"                # label path

MRMRINPUT="$INPUTFILE $TYPE $NFS $FORMAT $LABELIDX $SCORECLASS $LABELFILE"

$SPARK_HOME/bin/spark-submit \
    --class creggian.mrmr.Run \
    --master local[*] \
    --num-executors 1 \
    --executor-memory 4G \
    --driver-memory 4G \
    target/scala-2.10/mrmr_2.10-1.0.jar \
    $MRMRINPUT \
    2> /dev/null
```

##### LibSVM

```
INPUTFILE="data/mrmr.50x20.rw.x0_7.libsvm"                   # input path
TYPE="row-wise"                                              # alternative encoding
NFS="5"                                                      # number of feature to select
FORMAT="libsvm"                                              # LibSVM input format
LABELIDX="0"                                                 # index of the label
SCORECLASS="creggian.mrmr.feature.common.FeatureWiseMRMR"    # score class name
LABELFILE="data/mrmr.50x20.rw.x0_7.class.csv"                # label path

MRMRINPUT="$INPUTFILE $TYPE $NFS $FORMAT $LABELIDX $SCORECLASS $LABELFILE"

$SPARK_HOME/bin/spark-submit \
    --class creggian.mrmr.Run \
    --master local[*] \
    --num-executors 1 \
    --executor-memory 4G \
    --driver-memory 4G \
    target/scala-2.10/mrmr_2.10-1.0.jar \
    $MRMRINPUT \
    2> /dev/null
```
