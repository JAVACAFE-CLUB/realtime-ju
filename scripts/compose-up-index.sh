#!/usr/bin/env bash
set -euo pipefail

echo "🔥 Starting Index System Infrastructure..."

"$(dirname "$0")"/compose-up.sh index dev

echo ""
echo "✅ Index System Infrastructure is running!"
echo ""
echo "📊 Management URLs:"
echo "  - Kafka UI:         http://localhost:18080"
echo "  - Elasticsearch:    http://localhost:9200"
echo ""
echo "🚀 Now run: ./gradlew :index-system:bootRun"
echo ""


