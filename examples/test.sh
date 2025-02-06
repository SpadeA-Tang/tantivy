#!/bin/bash
# filepath: ./run_profile.sh
# 脚本用法: ./run_profile.sh [迭代次数]
# 默认迭代次数为 10

ITERATIONS=10
if [ -n "$1" ]; then
    ITERATIONS="$1"
fi

times=()
total=0

for ((i=1; i<=ITERATIONS; i++)); do
    echo "第 $i 次运行..."
    output=$(cargo run --release --package tantivy --example test_multi_index_write)
    # 从输出中提取类似 "10.595572709s" 的时间数字（单位秒）
    time_str=$(echo "$output" | grep -oE '[0-9]+\.[0-9]+s' | sed 's/s//')
    if [ -z "$time_str" ]; then
        echo "无法提取耗时，请检查输出格式。完整输出如下："
        echo "$output"
        exit 1
    fi
    times+=("$time_str")
    total=$(echo "$total + $time_str" | bc)
done

avg=$(echo "scale=6; $total / $ITERATIONS" | bc)

# 对时间数组进行排序
sorted=($(
    for t in "${times[@]}"; do
        echo "$t"
    done | sort -n
))
# 计算 p99 索引：ceil(ITERATIONS * 0.99) - 1 （转为 0 索引）
p99_idx=$(echo "$ITERATIONS*0.99" | bc -l)
p99_idx=$(printf "%.0f" "$(echo "($p99_idx+0.999999)/1" | bc -l)")
p99_idx=$(( p99_idx - 1 ))
if [ $p99_idx -ge $ITERATIONS ]; then
    p99_idx=$(( ITERATIONS - 1 ))
fi
p99=${sorted[$p99_idx]}

echo "平均耗时: ${avg}s"
echo "P99 耗时: ${p99}s"