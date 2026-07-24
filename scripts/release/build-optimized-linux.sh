#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$BASH_SOURCE")" && pwd -P)"
source "$SCRIPT_DIR/../lib/common.sh"

PACKAGE="pg-kinetic"
BIN="pg-kinetic"
WORK_DIR="${PG_KINETIC_OPTIMIZE_WORK_DIR:-${TMPDIR:-/tmp}/pg-kinetic-release-optimize-$$}"
TRAIN_COMMAND=""
FEATURES=()
USE_BOLT=false
DRY_RUN=false

while (($# > 0)); do
  case "$1" in
    --package)
      PACKAGE="$2"
      shift 2
      ;;
    --bin)
      BIN="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --train-command)
      TRAIN_COMMAND="$2"
      shift 2
      ;;
    --features)
      FEATURES+=(--features "$2")
      shift 2
      ;;
    --bolt)
      USE_BOLT=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

PROFILE_DIR="$WORK_DIR/pgo-profraw"
PROFILE_DATA="$WORK_DIR/pgo.profdata"
GENERATE_TARGET="$WORK_DIR/target-pgo-generate"
USE_TARGET="$WORK_DIR/target-pgo-use"
PGO_BINARY="$USE_TARGET/release/$BIN"
BOLT_DATA="$WORK_DIR/bolt.fdata"
BOLT_PERF_DATA="$WORK_DIR/perf.data"
BOLT_BINARY="$WORK_DIR/$BIN.bolt"
FEATURE_ARGS="${FEATURES[*]:-}"

if "$DRY_RUN"; then
  cat <<EOF
cargo build --release --locked -p $PACKAGE --bin $BIN $FEATURE_ARGS [RUSTFLAGS=-Cprofile-generate=$PROFILE_DIR]
LLVM_PROFILE_FILE=$PROFILE_DIR/$BIN-%p-%m.profraw PG_KINETIC_OPTIMIZED_BINARY=$GENERATE_TARGET/release/$BIN $TRAIN_COMMAND
llvm-profdata merge -o $PROFILE_DATA $PROFILE_DIR/*.profraw
cargo build --release --locked -p $PACKAGE --bin $BIN $FEATURE_ARGS [RUSTFLAGS=-Cprofile-use=$PROFILE_DATA]
EOF
  if "$USE_BOLT"; then
    cat <<EOF
perf record -e cycles:u -j any,u -o $BOLT_PERF_DATA -- env PG_KINETIC_OPTIMIZED_BINARY=$PGO_BINARY $TRAIN_COMMAND
perf2bolt $PGO_BINARY -p $BOLT_PERF_DATA -o $BOLT_DATA
llvm-bolt $PGO_BINARY -o $BOLT_BINARY -data=$BOLT_DATA -reorder-blocks=ext-tsp -reorder-functions=hfsort -split-functions -split-all-cold
EOF
  fi
  success "optimized release build dry-run"
  exit 0
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "optimized release builds require Linux" >&2
  exit 2
fi

if [[ -z "$TRAIN_COMMAND" ]]; then
  echo "--train-command is required" >&2
  exit 2
fi

require_command cargo >/dev/null
require_command llvm-profdata >/dev/null

if "$USE_BOLT"; then
  require_command perf >/dev/null
  require_command perf2bolt >/dev/null
  require_command llvm-bolt >/dev/null
fi

mkdir -p "$PROFILE_DIR"

run_from_repo_root env \
  CARGO_TARGET_DIR="$GENERATE_TARGET" \
  RUSTFLAGS="-Cprofile-generate=$PROFILE_DIR" \
  cargo build --release --locked -p "$PACKAGE" --bin "$BIN" "${FEATURES[@]}"

run_from_repo_root env \
  LLVM_PROFILE_FILE="$PROFILE_DIR/$BIN-%p-%m.profraw" \
  PG_KINETIC_OPTIMIZED_BINARY="$GENERATE_TARGET/release/$BIN" \
  bash -lc "$TRAIN_COMMAND"

shopt -s nullglob
profiles=("$PROFILE_DIR"/*.profraw)
if ((${#profiles[@]} == 0)); then
  echo "training command did not produce LLVM profile data in $PROFILE_DIR" >&2
  exit 1
fi

llvm-profdata merge -o "$PROFILE_DATA" "${profiles[@]}"

run_from_repo_root env \
  CARGO_TARGET_DIR="$USE_TARGET" \
  RUSTFLAGS="-Cprofile-use=$PROFILE_DATA" \
  cargo build --release --locked -p "$PACKAGE" --bin "$BIN" "${FEATURES[@]}"

if "$USE_BOLT"; then
  run_from_repo_root env \
    PG_KINETIC_OPTIMIZED_BINARY="$PGO_BINARY" \
    perf record -e cycles:u -j any,u -o "$BOLT_PERF_DATA" -- bash -lc "$TRAIN_COMMAND"
  perf2bolt "$PGO_BINARY" -p "$BOLT_PERF_DATA" -o "$BOLT_DATA"
  llvm-bolt "$PGO_BINARY" \
    -o "$BOLT_BINARY" \
    -data="$BOLT_DATA" \
    -reorder-blocks=ext-tsp \
    -reorder-functions=hfsort \
    -split-functions \
    -split-all-cold
  printf 'optimized binary: %s\n' "$BOLT_BINARY"
else
  printf 'optimized binary: %s\n' "$PGO_BINARY"
fi

success "optimized release build completed"
