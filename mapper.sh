#! /bin/bash
sudo apt update;
sudo apt -y install git
sudo apt -y python3.8
git clone https://github.com/pokearu/PythonMapReduce.git
cd PythonMapReduce
python3 mapper_node.py
