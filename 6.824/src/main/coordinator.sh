#!/bin/bash

rm -r tmp/
mkdir tmp
go run -race mrcoordinator.go pg*.txt
