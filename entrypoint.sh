#!/bin/bash
set -e


# Запускаем producer
/usr/local/bin/kafka-benchmark producer /app/config/base_producer.yaml 10B_bursts

# После завершения producer запускаем consumer
/usr/local/bin/kafka-benchmark consumer /app/config/base_consumer.yaml 10B_100MB 