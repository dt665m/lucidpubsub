#!/bin/sh
#MUST BE RUN ON DOCKER ENABLED MACHINE
hash docker 2>/dev/null || { echo >&2 "docker daemon not found, unable to start google pubsub emulator.  Test can still be run if google cloud-sdk is installed and the cloud-pubsub-emulator is running on host manually"; exit 1; }

export PUBSUB_EMULATOR_HOST="127.0.0.1:8085"
export PUBSUB_PROJECT_ID="testing"

#start google pubsub emulator in docker
docker run --rm --name pubsub -p 8085:8085 -d google/cloud-sdk /usr/lib/google-cloud-sdk/platform/pubsub-emulator/bin/cloud-pubsub-emulator --host=0.0.0.0 --port=8085

#test
go test -v -cover  -parallel 3

#cleanup
# docker stop postgres 
# docker stop adminer
docker stop pubsub
rm -rf ./badgerdb_test