#!/bin/bash
# run_tests.sh - Run all test files under tests/test_*.{q,sh}
# .q files run directly with q. .sh files run as bash scripts.
# Each test file runs in its own process; bash runner aggregates results.
#
# Usage (from project root):
#   ./tests/run_tests.sh
#   ./tests/run_tests.sh tests/test_schemas.q   # single test
#
# Exit code: 0 if all pass, 1 if any fail.

set -u  # error on unset vars (but NOT -e: we want to continue on test failure)

# Colors (disabled if not on a TTY)
if [[ -t 1 ]]; then
    RED=$'\033[0;31m'
    GREEN=$'\033[0;32m'
    YELLOW=$'\033[1;33m'
    NC=$'\033[0m'
else
    RED=""
    GREEN=""
    YELLOW=""
    NC=""
fi

# Always run from project root so relative paths in tests resolve.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT"

# Test file selection
if [[ $# -gt 0 ]]; then
    TESTS=("$@")
else
    shopt -s nullglob
    TESTS=(tests/test_*.q tests/test_*.sh)
    shopt -u nullglob
fi

if [[ ${#TESTS[@]} -eq 0 ]]; then
    echo "${YELLOW}No test files found matching tests/test_*.{q,sh}${NC}"
    exit 1
fi

echo "Running ${#TESTS[@]} test file(s)..."
echo ""

PASSED=0
FAILED=0
FAILED_TESTS=()

for test_file in "${TESTS[@]}"; do
    if [[ ! -f "$test_file" ]]; then
        echo "${RED}Missing: $test_file${NC}"
        FAILED=$((FAILED + 1))
        FAILED_TESTS+=("$test_file (missing)")
        continue
    fi

    case "$test_file" in
        *.q)
            output=$(q "$test_file" 2>&1 < /dev/null)
            ;;
        *.sh)
            output=$(bash "$test_file" 2>&1 < /dev/null)
            ;;
        *)
            echo "${YELLOW}Unknown test type: $test_file${NC}"
            continue
            ;;
    esac
    exit_code=$?

    if [[ $exit_code -eq 0 ]]; then
        echo "${GREEN}OK${NC}  $test_file"
        PASSED=$((PASSED + 1))
    else
        echo "${RED}FAIL${NC} $test_file (exit $exit_code)"
        echo "$output" | sed 's/^/    /'
        FAILED=$((FAILED + 1))
        FAILED_TESTS+=("$test_file")
    fi
done

echo ""
echo "============================================="
echo "Total: $((PASSED + FAILED)) | ${GREEN}Passed: $PASSED${NC} | ${RED}Failed: $FAILED${NC}"
echo "============================================="

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "  - $t"
    done
    exit 1
fi

exit 0
