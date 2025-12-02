#!/usr/bin/env bash
set -euo pipefail

echo "🔥 Starting Refine System Infrastructure..."

"$(dirname "$0")"/compose-up.sh refine dev

echo ""
echo "✅ Refine System Infrastructure is running!"
echo ""
echo "📊 Monitoring URLs:"
echo "  - Grafana:          http://localhost:18083 (admin/admin123)"
echo "  - Prometheus:       http://localhost:19090"
echo "  - Kafka Lag Export: http://localhost:19999/metrics"
echo "  - Kafka UI:         http://localhost:18080"
echo "  - Mongo Express:    http://localhost:18081 (admin/admin123)"
echo "  - Redis Commander:  http://localhost:18082"
echo ""
echo "🚀 Now run: ./gradlew :refine-system:bootRun"
echo ""


