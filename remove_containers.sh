#!/bin/bash 

docker stop $(docker ps -q)

docker rm -v $(docker ps -a -q)

docker rmi -f $(docker images -a -q)

docker volume prune

