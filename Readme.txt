hadoop fs -mkdir /input
hadoop fs -put C:\Users\sanju\Assignments\TODB\HW4\sample.csv /input

hadoop jar C:\Users\sanju\eclipse-workspace\Hadoop\target\Hadoop-0.0.1-SNAPSHOT.jar AgeYearCount /input/tedsa_puf_2000_2019.csv /cse532/output1

hadoop jar C:\Users\sanju\eclipse-workspace\Hadoop\target\Hadoop-0.0.1-SNAPSHOT.jar UniqueCBSA /input/tedsa_puf_2000_2019.csv /cse532/output8

spark-submit --class SparkAgeYearCount C:\Users\sanju\eclipse-workspace\Hadoop\target\Hadoop-0.0.1-SNAPSHOT.jar C:\Users\madhu\Assignments\TODB\HW4\tedsa_puf_2000_2019.csv /cse532/output6

spark-submit --class SparkAgeYearCount C:\Users\sanju\eclipse-workspace\Hadoop\target\Hadoop-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/inputtedsa_puf_2000_2019.csv hdfs://localhost:9000/output2

spark-submit C:\Users\sanju\Assignments\TODB\HW4\SparkTopCountCBSA.py

hadoop jar C:\Users\sanju\eclipse-workspace\Hadoop\target\Hadoop-0.0.1-SNAPSHOT.jar SparkTopCountCBSA /input/tedsa_puf_2000_2019.csv /cse532/output9
