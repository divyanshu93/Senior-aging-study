#!/bin/bash

#$1 : Input path of train folder eg. /user/dxp151630/train
#$2 : Output path of folder eg. /user/dxp151630/output
var="0000"

for i in {1..9}
  do
     location=$var$i
     hadoop jar FinalProject.jar FinalProject.FinalTry $1/$location/targets.csv $1/$location/columns.csv $2/$location
 done


#hadoop jar FinalProject.jar FinalProject.FinalTry $1 $2 $3
hadoop jar FinalProject.jar FinalProject.FinalTry $1/00010/targets.csv $1/00010/columns.csv $2/00010

hdfs dfs -get {$2}


