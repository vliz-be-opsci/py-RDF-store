#! /usr/bin/env bash

DCKRIMAGE="ghcr.io/vliz-be-opsci/kgap/kgap_graphdb:latest"

if [ -z $(which docker) ]; then
    echo "ERROR :: Need docker installed to run $0 - exiting."
    exit 1;
fi

if [ -z $(docker images -q ${DCKRIMAGE} 2>/dev/null) ]; then
    echo "Required Docker Image '${DCKRIMAGE}' missing - trying to pull"
    docker pull -q ${DCKRIMAGE}
fi 

if [ -z $(docker images -q ${DCKRIMAGE} 2>/dev/null) ]; then
    echo "ERROR :: Need docker image '${DCKRIMAGE}' to run $0 - exiting."
    exit 1;
fi 

DCKRNAME=${DCKRNAME:-rdf_store_test}
REPONAME=${REPONAME:-rdf_store_test}

cmd=$1


# Function to start the test-container
do_start() {
    echo "launching local graphdb from docker image"
    docker run -d --rm --name ${DCKRNAME} -e GDB_REPO=${REPONAME} -p 7200:7200 ghcr.io/vliz-be-opsci/kgap/kgap_graphdb:latest
    echo "docker 'graphdb' started"
    echo "contact it at http://localhost:7200 and/or use these settings for SPARQL connections:"
    TEST_SPARQL_READ_URI=http://localhost:7200/repositories/${REPONAME}
    TEST_SPARQL_WRITE_URI=http://localhost:7200/repositories/${REPONAME}/statements
    echo "for tests connecting to the instance use:"
    echo "   TEST_SPARQL_READ_URI=${TEST_SPARQL_READ_URI}"
    echo "  TEST_SPARQL_WRITE_URI=${TEST_SPARQL_WRITE_URI}"
}

# Function to stop the container
do_stop() {
    echo "shutting-down local graphdb docker container"
    docker stop ${DCKRNAME} || (echo "Aborted script to stop ${DCKRNAME}" && exit 1)
    echo "docker 'graphdb' stopped"
}

# Function to wait for the container
do_wait_health() {
    echo "waiting for local graphdb docker container to be in health state"
    status=""
    wait=0
    while [[ $status != "healthy" ]] ; do 
        sleep $wait; echo -n "."
        status=$(docker inspect -f {{.State.Health.Status}} ${DCKRNAME})
        wait=0.5
    done; 
    echo -e "\ndocker 'graphdb' up and $status"
}

# Main execution
case $cmd in
    "start")
        do_start
        ;;
    "wait")
        do_wait_health
        ;;
    "start-wait")
        do_start
        do_wait_health
        ;;
    "stop")
        do_stop
        ;;
    *)
        echo "Invalid command. Use 'start' or 'stop'."
        ;;
esac
