#!/usr/bin/env bash
# trace_and_run.sh — runs one MinioOneShotRead query while mc admin trace captures
# every S3 request into a JSONL file for offline analysis.
#
# Usage:
#   scripts/trace_and_run.sh <lance|parquet> <prune|fullscan> [direct|proxy] \
#                            [cache=true|false] [profile=off|cpu|wall|alloc]
#
# Output:
#   /tmp/trace-<format>-<query>-<tag>-<timestamp>.jsonl   (raw mc trace output)
#   /tmp/oneshot-<format>-<query>-<tag>-<timestamp>.log   (Spark driver stdout/stderr)
#   /tmp/oneshot-<format>-<query>-<profile>-<epoch>.html  (async-profiler flamegraph, if profile != off)

set -u
if [[ $# -lt 2 || $# -gt 5 ]]; then
  echo "usage: $0 <lance|parquet> <prune|fullscan> [direct|proxy] [cache=true|false] [profile=off|cpu|wall|alloc]" >&2
  exit 1
fi

FORMAT="$1"
QUERY="$2"
MODE="${3:-direct}"
CACHE="${4:-true}"
PROFILE="${5:-off}"
TAG="${MODE}-cache${CACHE}"
if [[ "$PROFILE" != "off" ]]; then
  TAG="${TAG}-prof${PROFILE}"
fi
TS="$(date +%Y%m%d-%H%M%S)"
TRACE_FILE="/tmp/trace-${FORMAT}-${QUERY}-${TAG}-${TS}.jsonl"
LOG_FILE="/tmp/oneshot-${FORMAT}-${QUERY}-${TAG}-${TS}.log"
CP_FILE="/tmp/micro-bench-cp.txt"
ASPROF_JAR="/opt/homebrew/Cellar/async-profiler/4.4/libexec/async-profiler.jar"

if [[ ! -f "$CP_FILE" ]]; then
  echo "classpath file missing: $CP_FILE — run 'mvn dependency:build-classpath -Dmdep.outputFile=$CP_FILE -q' first" >&2
  exit 1
fi

CP="target/classes:$(cat "$CP_FILE")"
if [[ "$PROFILE" != "off" ]]; then
  if [[ ! -f "$ASPROF_JAR" ]]; then
    echo "async-profiler jar missing: $ASPROF_JAR — install with 'brew install async-profiler' or set ASPROF_JAR" >&2
    exit 1
  fi
  CP="${CP}:${ASPROF_JAR}"
fi

echo "[1/4] starting mc admin trace → $TRACE_FILE"
mc admin trace local --call s3 --json > "$TRACE_FILE" 2>/dev/null &
TRACE_PID=$!
# Give mc time to establish the streaming connection before the query fires
sleep 2
if ! kill -0 "$TRACE_PID" 2>/dev/null; then
  echo "mc admin trace exited immediately; see $TRACE_FILE" >&2
  exit 1
fi
echo "       trace pid=$TRACE_PID"

echo "[2/4] running MinioOneShotRead $FORMAT $QUERY $MODE $CACHE profile=$PROFILE → $LOG_FILE"
java -Xms4g -Xmx4g \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -Dio.netty.tryReflectionSetAccessible=true \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+DebugNonSafepoints \
  -cp "$CP" \
  org.lance.spark.microbenchmark.MinioOneShotRead "$FORMAT" "$QUERY" "$MODE" "$CACHE" "$PROFILE" \
  > "$LOG_FILE" 2>&1
RC=$?
echo "       exit=$RC"

echo "[3/4] stopping trace"
# Drain briefly so in-flight trace events land in the file before kill
sleep 1
kill "$TRACE_PID" 2>/dev/null
wait "$TRACE_PID" 2>/dev/null

echo "[4/4] done."
echo "  trace:  $TRACE_FILE  ($(wc -l < "$TRACE_FILE") lines)"
echo "  driver: $LOG_FILE    ($(wc -l < "$LOG_FILE") lines)"

if [[ "$PROFILE" != "off" ]]; then
  JFR_FILE="$(grep -o '/tmp/oneshot-[^ ]*\.jfr' "$LOG_FILE" | tail -1)"
  if [[ -n "$JFR_FILE" && -f "$JFR_FILE" ]]; then
    HTML_FILE="${JFR_FILE%.jfr}.html"
    COLLAPSED_FILE="${JFR_FILE%.jfr}.collapsed"
    JFR_CONVERTER="/opt/homebrew/Cellar/async-profiler/4.4/libexec/jfr-converter.jar"
    java -jar "$JFR_CONVERTER" "$JFR_FILE" "$HTML_FILE" >/dev/null 2>&1
    java -jar "$JFR_CONVERTER" --output collapsed "$JFR_FILE" "$COLLAPSED_FILE" >/dev/null 2>&1
    echo "  jfr:       $JFR_FILE       ($(du -h "$JFR_FILE" | awk '{print $1}'))"
    [[ -f "$HTML_FILE" ]]      && echo "  html:      $HTML_FILE  ($(du -h "$HTML_FILE" | awk '{print $1}'))"
    [[ -f "$COLLAPSED_FILE" ]] && echo "  collapsed: $COLLAPSED_FILE  ($(wc -l < "$COLLAPSED_FILE") stacks)"
  fi
fi
