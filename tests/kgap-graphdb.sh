#! /usr/bin/env bash

if [ -z $(which docker) ]; then
    echo "Need docker installed to run $0"
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
    export TEST_SPARQL_READ_URI=http://localhost:7200/repositories/${REPONAME}
    export TEST_SPARQL_WRITE_URI=http://localhost:7200/repositories/${REPONAME}/statements
    echo "   TEST_SPARQL_READ_URI=${TEST_SPARQL_READ_URI}"
    echo "  TEST_SPARQL_WRITE_URI=${TEST_SPARQL_WRITE_URI}"
    echo "env settings exported."
}

# Function to stop the container
do_stop() {
    echo "shutting-down local graphdb docker container"
    docker stop ${DCKRNAME} || echo "Aborted script to stop ${DCKRNAME}" && exit 1
    echo "docker 'graphdb' stopped"
    unset TEST_SPARQL_READ_URI
    unset TEST_SPARQL_WRITE_URI
    echo "env settings removed."
}

# Main execution

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]
then
    echo "Warning:"
    echo "  Script is being run directly. This is not recommended. Instead consider running in source context like this:"
    echo "  $ source $0 $1"
else
    echo "Script is being sourced. This will make environment changes last beyond execution."
fi
echo

case $cmd in
    "start")
        do_start
        ;;
    "stop")
        do_stop
        ;;
    *)
        echo "Invalid command. Use 'start' or 'stop'."
        ;;
esac
