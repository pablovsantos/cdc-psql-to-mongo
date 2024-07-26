#!/bin/bash

host="$1"
port="$2"
shift 2
cmd="$@"

until nc -z -v -w30 $host $port
do
  echo "Waiting for database connection at $host:$port..."
  sleep 5
done

