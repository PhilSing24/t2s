#!/bin/bash
# run_tests.sh - Run all q tests under tests/test_*.q
# Each test runs in its own q process. Bash runner aggregates results.
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
    # Glob can fail if no tests exist; check first
    shopt -s nullglob
    TESTS=(tests/test_*.q)
    shopt -u nullglob
fi

if [[ ${#TESTS[@]} -eq 0 ]]; then
    echo "${YELLOW}No test files found matching tests/test_*.q${NC}"
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

    # Run the test. The q process exits 0/1 via .t.finish[].
    # Capture output so we can show it on failure.
    output=$(q "$test_file" 2>&1 < /dev/null)
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
