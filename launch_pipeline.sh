#!/bin/sh

# Author: Shawn Wang
# Email: wshawn2020@gmail.com
# Github: https://github.com/wshawn2020

echo "[1/12] Start building up conda environment"
conda env create -f env.yml
conda env list
echo "[2/12] Finish building up conda environment"

echo "[3/12] Start to activate demo conda environment"
source activate demo
conda env list
echo "[4/12] Activate demo environment done"

echo "[5/12]Start running docker containers"
docker-compose up -d
docker ps
echo "[6/12]Containers are running"

echo "[7/12] Start running assignment pipeline"
python ./script/main.py
echo "[8/12] Finish assignment pipeline"

echo "[9/12] Stopping docker containers"
docker-compose down
echo "[10/12] Docker containers stopped"

echo "[11/12] Deactivate conda environment"
source conda deactivate
conda env list
echo "[12/12] Deactivate done"
