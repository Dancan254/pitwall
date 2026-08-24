#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/.run"
LOG_DIR="$RUN_DIR/logs"
STREAMS_STATE_DIR="${TMPDIR:-/tmp}/pitwall-kafka-streams"

SERVICES=(pitwall-stream-processor pitwall-serving pitwall-source)
declare -A PORTS=(
  [pitwall-stream-processor]=8082
  [pitwall-serving]=8083
  [pitwall-source]=8081
)
declare -A HEALTH_PATHS=(
  [pitwall-stream-processor]=/actuator/health
  [pitwall-serving]=/actuator/health
  [pitwall-source]=/api/v1/source/status
)

RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; PINK=$'\e[95m'; DIM=$'\e[2m'; OFF=$'\e[0m'

say()  { printf '%s\n' "$*"; }
step() { printf '%s==>%s %s\n' "$PINK" "$OFF" "$*"; }
ok()   { printf '  %s✓%s %s\n' "$GREEN" "$OFF" "$*"; }
warn() { printf '  %s!%s %s\n' "$YELLOW" "$OFF" "$*"; }
die()  { printf '%serror:%s %s\n' "$RED" "$OFF" "$*" >&2; exit 1; }

wait_for() {
  local label=$1 check=$2 timeout=${3:-180} waited=0
  while ! eval "$check" >/dev/null 2>&1; do
    if (( waited >= timeout )); then
      printf '\n'; die "$label did not come up within ${timeout}s"
    fi
    printf '\r  %s…%s waiting for %s (%ss)' "$DIM" "$OFF" "$label" "$waited"
    sleep 3; waited=$((waited + 3))
  done
  printf '\r\033[K'; ok "$label"
}

pid_of() { local s=$1; [[ -f "$RUN_DIR/$s.pid" ]] && cat "$RUN_DIR/$s.pid" || true; }

is_running() {
  local pid; pid=$(pid_of "$1")
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

infra_up() {
  step "Infrastructure"
  docker compose -f "$ROOT/docker-compose.yml" up -d >/dev/null
  for c in pitwall-kafka pitwall-postgres pitwall-otel-lgtm; do
    wait_for "$c" "[ \"\$(docker inspect --format='{{.State.Health.Status}}' $c 2>/dev/null)\" = healthy ]"
  done
}

build_if_needed() {
  local missing=0
  for s in "${SERVICES[@]}"; do
    [[ -f "$ROOT/$s/target/$s-0.0.1-SNAPSHOT.jar" ]] || missing=1
  done
  if [[ "${PITWALL_BUILD:-}" == "1" || $missing == 1 ]]; then
    step "Building (skipping tests)"
    (cd "$ROOT" && ./mvnw -q clean install -DskipTests) || die "build failed"
    ok "all modules packaged"
  else
    step "Using existing jars ${DIM}(PITWALL_BUILD=1 to rebuild)${OFF}"
  fi
}

start_service() {
  local service=$1; shift
  if is_running "$service"; then
    warn "$service already running (pid $(pid_of "$service"))"
    return
  fi
  local jar="$ROOT/$service/target/$service-0.0.1-SNAPSHOT.jar"
  [[ -f "$jar" ]] || die "missing $jar, run with PITWALL_BUILD=1"
  setsid nohup java -jar "$jar" "$@" > "$LOG_DIR/$service.log" 2>&1 < /dev/null &
  echo $! > "$RUN_DIR/$service.pid"
}

cmd_start() {
  local profile=${1:-}
  local source_args=()
  if [[ -n "$profile" ]]; then
    case "$profile" in
      dev|race|breakit) source_args=(--spring.profiles.active="$profile") ;;
      *) die "unknown profile '$profile' (dev, race, breakit)" ;;
    esac
  fi

  mkdir -p "$LOG_DIR"
  infra_up
  build_if_needed

  step "Services"
  start_service pitwall-stream-processor
  wait_for "pitwall-stream-processor :8082" "curl -sf localhost:8082/actuator/health"
  start_service pitwall-serving
  wait_for "pitwall-serving :8083" "curl -sf localhost:8083/actuator/health"
  start_service pitwall-source "${source_args[@]}"
  wait_for "pitwall-source :8081" "curl -sf localhost:8081/api/v1/source/status"

  printf '\n%sPitwall is up%s%s\n' "$PINK" "$OFF" "${profile:+ (source profile: $profile)}"
  cat <<EOF

  Dashboard    http://localhost:8083
  Grafana      http://localhost:3000
  Source API   http://localhost:8081/api/v1/source/status

  logs         ./scripts/pitwall.sh logs [service]
  stop         ./scripts/pitwall.sh stop
EOF
}

cmd_stop() {
  step "Stopping services"
  for service in "${SERVICES[@]}"; do
    local pid; pid=$(pid_of "$service")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in {1..15}; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
      kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
      ok "$service stopped"
    else
      warn "$service was not running"
    fi
    rm -f "$RUN_DIR/$service.pid"
  done
  if [[ "${1:-}" == "--all" ]]; then
    step "Stopping infrastructure"
    docker compose -f "$ROOT/docker-compose.yml" down >/dev/null
    ok "containers down"
  fi
}

cmd_status() {
  step "Services"
  for service in "${SERVICES[@]}"; do
    local port=${PORTS[$service]}
    if is_running "$service"; then
      local code; code=$(curl -s -o /dev/null -m 2 -w '%{http_code}' "localhost:$port${HEALTH_PATHS[$service]}" || echo 000)
      if [[ "$code" == 200 ]]; then ok "$service  :$port  pid $(pid_of "$service")"
      else warn "$service  :$port  pid $(pid_of "$service")  not answering (HTTP $code)"; fi
    else
      warn "$service  :$port  stopped"
    fi
  done
  step "Containers"
  docker compose -f "$ROOT/docker-compose.yml" ps --format 'table {{.Name}}\t{{.Status}}' 2>/dev/null || true
}

resolve_service() {
  local name=$1
  case "$name" in
    source|processor|stream-processor|serving) echo "pitwall-${name#stream-}" ;;
    *) echo "$name" ;;
  esac
}

cmd_logs() {
  local service=${1:-}
  if [[ -z "$service" ]]; then
    tail -n 40 -f "$LOG_DIR"/*.log
    return
  fi
  service=$(resolve_service "$service")
  [[ -f "$LOG_DIR/$service.log" ]] || die "no log for '$1', try: source, processor, serving"
  tail -n 200 -f "$LOG_DIR/$service.log"
}

cmd_reset() {
  cmd_stop
  step "Clearing Kafka topics and Streams state"

  # Before the application topics, so the tool can still resolve them, and with
  # --force because stopping the services does not expire the consumer group
  # members straight away and the tool refuses while the group has any.
  local reset_output
  reset_output=$(docker exec pitwall-kafka /opt/kafka/bin/kafka-streams-application-reset.sh \
    --bootstrap-server localhost:9092 --application-id pitwall-rollups --force 2>&1) || true

  for topic in telemetry.events telemetry.rollups telemetry.alerts; do
    docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic "$topic" >/dev/null 2>&1 || true
  done
  rm -rf "$STREAMS_STATE_DIR"

  # Recreated here rather than left to the services. Only pitwall-source declares
  # telemetry.events, but the processor starts first, and its Streams client meets
  # a missing source topic at rebalance and shuts the client down for good
  # (MissingSourceTopicException). Partition counts match the NewTopic beans.
  for spec in telemetry.events:12 telemetry.rollups:12 telemetry.alerts:3; do
    docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "${spec%:*}" --partitions "${spec#*:}" --replication-factor 1 >/dev/null 2>&1 || true
  done

  # Verified rather than trusted: a surviving suppress-state-store changelog
  # replays old windows into telemetry.rollups on the next start, and they arrive
  # looking exactly like live data.
  local leftover
  leftover=$(docker exec pitwall-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c '^pitwall-rollups-' || true)
  if (( leftover > 0 )); then
    say "$reset_output" >&2
    die "$leftover Kafka Streams internal topics survived the reset"
  fi

  ok "topics recreated empty, Streams internal topics and state cleared"
  warn "telemetry_event rows are kept; truncate manually if you want a clean store"
}

usage() {
  cat <<EOF
Pitwall: run the whole platform

  ./scripts/pitwall.sh start [dev|race|breakit]   infra, then all three services
  ./scripts/pitwall.sh stop [--all]               stop services (--all also stops containers)
  ./scripts/pitwall.sh restart [profile]
  ./scripts/pitwall.sh status
  ./scripts/pitwall.sh logs [service]
  ./scripts/pitwall.sh reset                      stop, then wipe Kafka topics and Streams state

  PITWALL_BUILD=1   force a rebuild before starting
EOF
}

mkdir -p "$RUN_DIR" "$LOG_DIR"
case "${1:-start}" in
  start)   shift || true; cmd_start "${1:-}" ;;
  stop)    shift || true; cmd_stop "${1:-}" ;;
  restart) shift || true; cmd_stop; cmd_start "${1:-}" ;;
  status)  cmd_status ;;
  logs)    shift || true; cmd_logs "${1:-}" ;;
  reset)   cmd_reset ;;
  -h|--help|help) usage ;;
  *) usage; exit 1 ;;
esac
