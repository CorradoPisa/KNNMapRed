# KNNMapRed

## Execution:
- copy the datesets to HDFS
- execute:
```
hadoop jar target/KNNMapRed-1.0-SNAPSHOT.jar it.cnr.isti.pad.KNNMapRed testD.csv output
hadoop fs -cat output/part* | awk '{ sum += $2 } END { if (NR > 0) print sum / NR }'
```
