#!/bin/bash

LOG_FILE="system_usage.log"
echo "Timestamp,CPU_Usage(%),Memory_Usage(%)" > "$LOG_FILE"

log_usage() {
    while true; do
        TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
        CPU_USAGE=$(top -bn2 | grep "Cpu(s)" | tail -n 1 | awk '{print $2 + $4}')
        MEM_USAGE=$(free | awk '/Mem:/ {printf "%.2f", $3/$2 * 100}')
        echo "$TIMESTAMP,$CPU_USAGE,$MEM_USAGE" >> "$LOG_FILE"
        sleep 10
    done
}

log_usage
