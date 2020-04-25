#!/usr/bin/env bash
image_name=stp-etl

docker build . --rm -t $image_name --no-cache
