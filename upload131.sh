#!/bin/bash

file=$1
tofile=$2

#scp -P 12598 ./src/main/scala/com/xiaoi/spark/${file}  hadoop@122.226.240.131:/xiaoi/joshTest/iLearning/code/src/main/scala/com/xiaoi/spark/${tofile}
scp -P 12598 ./target/scala-2.11/ilearning_2.11-0.1.jar  hadoop@122.226.240.131:/xiaoi/joshTest/iLearning/code/target/scala-2.11

echo "upload sucess..."
