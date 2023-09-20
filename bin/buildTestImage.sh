#!/bin/bash

scriptPos=${0%/*}


imageBase=ghcr.io/okieoth/rabbitmqadmin
imageTag=`cat $scriptPos/../assets/version.txt | grep -P '\d+\.\d+\.\d+'`

imageName="$imageBase:$imageTag"

echo "I am going to create: $imageName"

pushd "$scriptPos" > /dev/null
	if docker build -t $imageName ../assets
    then
        docker tag $imageName $imageBase
        echo -en "\033[1;34m  image created: $imageName, $imageBase \033[0m\n"
    else
        echo -en "\033[1;31m  error while create image: $imageName \033[0m\n"
    fi
popd > /dev/null
