#!/usr/bin/env bash
set -euo pipefail

echo "🔥 Starting Serving System Infrastructure..."

"$(dirname "$0")"/compose-up.sh serving dev

echo ""
echo "✅ Serving System Infrastructure is running!"
echo ""
echo "📊 Management URLs:"
echo "  - Redis Commander:  http://localhost:18082"
echo "  - Elasticsearch:    http://localhost:9200"
echo ""
echo "🚀 Now run: ./gradlew :serving-system:bootRun"
echo ""


