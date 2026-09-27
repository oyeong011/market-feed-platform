#!/usr/bin/env bash
#
# 개발용 비밀 파일 생성기.
#
# compose 파일들이 `./ops/secrets/*` 를 읽는데, 그 파일을 만드는 절차가 저장소에
# 어디에도 없었다. 없는 경로를 바인드 마운트하면 도커는 그 자리에 **디렉터리**를
# 만들고, 비밀번호 파일을 기대하는 프로세스는 디렉터리를 읽는다. `make obs-up` 과
# `make docker-up` 이 그래서 한 번도 제대로 뜬 적이 없다(결함 48).
#
# **개발용이다.** 운영에서는 이 파일을 쓰지 말고 비밀 관리자가 같은 경로에 넣는다.
# 여기서 만든 값은 저장소 밖으로 나가지 않는다 — ops/secrets/ 는 .gitignore 에 있다.
#
#   bash ops/gen_dev_secrets.sh          # 없는 것만 만든다
#   bash ops/gen_dev_secrets.sh --force  # 다시 만든다
#   bash ops/gen_dev_secrets.sh --list   # 만드는 경로만 출력한다 (시험이 읽는다)
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/ops/secrets"

# 이 목록이 곧 범위다. compose 가 읽는 호스트 경로를 여기에 적어 두고,
# tests/test_compose_bind_mounts.py 가 "compose 가 읽는 것과 이 목록이 같은가"를 본다.
# 목록에 없는 경로를 compose 가 읽으면 그 시험이 실패한다.
PATHS=(
  "ops/secrets/grafana_admin_password"
  "ops/secrets/postgres_password"
)

if [ "${1:-}" = "--list" ]; then
  printf '%s\n' "${PATHS[@]}"
  exit 0
fi

FORCE=0
[ "${1:-}" = "--force" ] && FORCE=1

mkdir -p "$DIR"
chmod 700 "$DIR"

random_secret() {
  # openssl 이 없는 기계도 있다. 표준 도구만으로 32바이트 난수를 16진수로 만든다.
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
  else
    LC_ALL=C tr -dc 'a-f0-9' < /dev/urandom | head -c 64
    echo
  fi
}

made=0
kept=0
for rel in "${PATHS[@]}"; do
  target="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/$rel"
  if [ -d "$target" ]; then
    # 도커가 만들어 둔 디렉터리다. 이게 있는 채로는 절대 안 뜬다.
    echo "  치움  $rel (도커가 만든 디렉터리 — 비밀번호 파일 자리에 디렉터리가 있었다)"
    rmdir "$target" 2>/dev/null || rm -rf "$target"
  fi
  if [ -f "$target" ] && [ "$FORCE" -eq 0 ]; then
    kept=$((kept + 1))
    echo "  유지  $rel"
    continue
  fi
  random_secret > "$target"
  chmod 600 "$target"
  made=$((made + 1))
  echo "  생성  $rel"
done

echo
echo "생성 $made · 유지 $kept · 위치 ops/secrets/ (0700, .gitignore 대상)"
if [ -f "$DIR/grafana_admin_password" ]; then
  echo "Grafana 로그인:  admin / $(cat "$DIR/grafana_admin_password")"
fi
