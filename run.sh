#!/bin/bash
set -euo pipefail

# .env가 있으면 읽는다. 서버 시작에는 DB가 필요 없다.
if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${CATALOG_PATH:?CATALOG_PATH가 필요합니다 (서버 catalog.json 경로)}"
: "${SONG_MANIFEST_PATH:?SONG_MANIFEST_PATH가 필요합니다 (같은 버전 manifest 경로)}"
export CATALOG_PATH SONG_MANIFEST_PATH

go run ./cmd/server
