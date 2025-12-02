#!/usr/bin/env bash
set -euo pipefail

echo "🔥 Starting All Systems Infrastructure..."

# 모든 시스템용 프로파일 + 개발용 kafka-ui 포함
"$(dirname "$0")"/compose-up.sh collector refine index serving dev

echo ""
echo "✅ All Systems Infrastructure is running!"
echo ""
echo "📊 Monitoring URLs:"
echo "  - Grafana:          http://localhost:18083 (admin/admin123)"
echo "  - Prometheus:       http://localhost:19090"
echo "  - Kafka Lag Export: http://localhost:19999/metrics"
echo "  - Kafka UI:         http://localhost:18080"
echo "  - Mongo Express:    http://localhost:18081 (admin/admin123)"
echo "  - Redis Commander:  http://localhost:18082"
echo ""
echo "🚀 Now you can run any system:"
echo "   ./gradlew :collector-system:bootRun"
echo "   ./gradlew :refine-system:bootRun"
echo "   ./gradlew :index-system:bootRun"
echo "   ./gradlew :serving-system:bootRun"
echo ""


