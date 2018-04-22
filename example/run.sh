#!/bin/sh
#MUST BE RUN ON DOCKER ENABLED MACHINE
hash docker 2>/dev/null || { echo >&2 "docker daemon not found, unable to start google pubsub emulator.  Test can still be run if google cloud-sdk is installed and the cloud-pubsub-emulator is running on host manually"; exit 1; }

export PUBSUB_EMULATOR_HOST="127.0.0.1:8085"
export PUBSUB_PROJECT_ID="testing"

POSTGRES_CONTAINER="postgres"
PASSWORD="123456"

#start postgres
docker run --rm -p 5432:5432 --name $POSTGRES_CONTAINER -e POSTGRES_PASSWORD=$PASSWORD -d postgres
docker run --rm --name adminer --link $POSTGRES_CONTAINER:db -p 8080:8080 -d adminer
sleep 2s
docker cp structure.sql postgres:/structure.sql
docker exec postgres psql -U $CONTAINER postgres -f /structure.sql 

#start google pubsub emulator in docker
docker run --rm --name pubsub -p 8085:8085 -d google/cloud-sdk /usr/lib/google-cloud-sdk/platform/pubsub-emulator/bin/cloud-pubsub-emulator --host=0.0.0.0 --port=8085

#run
go run main.go

#cleanup
docker stop postgres adminer pubsub
