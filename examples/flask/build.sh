#!/bin/bash
set -e

SRC_PATH=`readlink -f ../../`
CP_FILES="${SRC_PATH}/requirements.txt ${SRC_PATH}/setup.py ${SRC_PATH}/README.rst"
TEMP_DIR="./dinao-tmp/"

function cleanup {
    rm -rf ${TEMP_DIR}
}
trap cleanup EXIT

rm -rf  ${TEMP_DIR}; mkdir ${TEMP_DIR}
cp -r ${SRC_PATH}/dinao ${TEMP_DIR}/dinao
cp ${CP_FILES} ${TEMP_DIR}

docker compose build
