#!/usr/bin/env bash
set -euo pipefail

# 모든 시스템용 프로파일 + 개발용 kafka-ui 포함
"$(dirname "$0")"/compose-up.sh collector refine index serving dev


