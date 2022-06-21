#!/bin/bash

# validate postgres
echo "sleeping for 5 seconds during postgres boot..."
sleep 5
PGPASSWORD=wds psql --username wds -d wds -c "SELECT VERSION();SELECT NOW()"
