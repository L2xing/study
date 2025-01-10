#!/bin/bash

bash --version

# 启动几个后台作业
echo "启动后台作业1"
sleep 5 &
job1_pid=$!

echo "启动后台作业2"
sleep 3 &
job2_pid=$!

echo "启动后台作业3"
sleep 7 &
job3_pid=$!

# 使用wait -n等待第一个作业完成
echo "等待第一个作业完成"
wait -n

# 检查哪个作业完成了
if [ $? -eq 0 ]; then
    echo "作业$job1_pid完成"
elif [ $? -eq $job2_pid ]; then
    echo "作业$job2_pid完成"
elif [ $? -eq $job3_pid ]; then
    echo "作业$job3_pid完成"
else
    echo "未知作业完成"
fi

# 等待所有作业完成
echo "等待所有作业完成"
wait

echo "所有作业已完成"
