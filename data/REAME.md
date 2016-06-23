
 MRMRINPUT="mrmr.50x20.cw.c0.x1_8.csv column-wise 10 0,1 0,1 , 0"
 spark-submit --class creggian.mrmr.Run --master local[*] --num-executors 1 --executor-memory 4G --driver-memory 4G mrmr_2.10-1.0.jar $MRMRINPUT
