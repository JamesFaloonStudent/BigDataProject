
#!/bin/bash


cd ~/Project/data/"Final Dataset"

hdfs dfs -mkdir -p /raw/hue
echo "Created /raw/hue in hdfs"
hdfs dfs -put consumption_*.txt /raw/hue/
echo "put consumption files in /raw/hue"

