#!/usr/bin/env bash

go build -buildmode=plugin ../../mrapps/indexer.go

go run ../mrworker.go ./indexer.so