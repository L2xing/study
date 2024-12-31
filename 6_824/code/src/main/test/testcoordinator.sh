#!/usr/bin/env bash
rm coo.log
rm work.log

# 构建
go build ../mrcoordinator.go

# 启动
timeout -k 1s 1m ./mrcoordinator ../pg*txt > coo.log &


# 构建 worker
go build ../mrworker.go

go build -buildmode=plugin ../../mrapps/indexer.go

timeout -k 1s 1m ./mrworker ./indexer.so > work.log &


