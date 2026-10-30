#!/bin/bash
# Usage:
#   ./run.sh          run all exercises
#   ./run.sh 3        run only Exercise 3
#   ./run.sh 1 4      run Exercises 1 and 4
#   ./run.sh test     run the unit tests
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "test" ]]; then
  mvn -q test
else
  mvn -q compile exec:java -Dexec.args="$*"
fi
