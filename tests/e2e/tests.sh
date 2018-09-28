#!/usr/bin/env bash

set -e
set -x
export SHELLOPTS


function generate_user_token() {
    local name=$1
    docker exec platformstorageapi_auth_server_1 platform-storage-make-token $name
}

function create_regular_user() {
    local name=$1
    local data="{\"name\": \"$name\"}"
    curl --fail --data "$data" -H "Authorization: Bearer $ADMIN_TOKEN" \
        http://localhost:5003/api/v1/users
}

function list_home_directory() {
    local user=$1
    local token=$2
    curl --fail -H "Authorization: Bearer $token" \
        http://localhost:5000/api/v1/storage/$1/
}


ADMIN_TOKEN=$(generate_user_token admin)

USER_NAME=$(uuidgen | awk '{print tolower($0)}')
USER_TOKEN=$(generate_user_token $USER_NAME)

wait_for_registry
create_regular_user $USER_NAME
list_home_directory $USER_NAME $USER_TOKEN
