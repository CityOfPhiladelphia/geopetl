#!/usr/bin/env bash

# If these two variables exist..
if [[ -z "$AWS_ACCESS_KEY_ID" && -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error, S3 credentials not declared! Please set them."
    return 1
fi


if [[ ! -f "oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm" ]]; then
# instant basic-lite instant oracle client
    aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm || return 1 
else
    echo "RPM already exists"
fi


if [[ ! -f "oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm" ]]; then
# instant oracle-sdk
    aws s3api get-object \
    --bucket citygeo-oracle-instant-client \
    --key oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
        oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm || return 1
else
    echo "RPM already exists"
fi
