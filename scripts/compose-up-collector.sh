#!/usr/bin/env bash
set -euo pipefail

echo "🔥 Starting Collector System Infrastructure..."

"$(dirname "$0")"/compose-up.sh collector dev

echo ""
echo "✅ Collector System Infrastructure is running!"
echo ""
echo "📊 Management URLs:"
echo "  - Kafka UI:         http://localhost:18080"
echo ""
echo "🚀 Now run: ./gradlew :collector-system:bootRun"
echo ""


