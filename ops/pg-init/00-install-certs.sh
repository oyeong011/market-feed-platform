#!/bin/bash
#
# 서버 인증서를 $PGDATA 로 복사한다.
#
# Postgres 는 ssl_key_file 이 **자기 소유이고 0600** 이 아니면 기동을 거부한다.
# 볼륨에 만든 파일의 소유자를 맞추려면 이미지의 postgres uid 를 알아야 하는데,
# 그 값은 이미지마다 다르다(debian 999 · alpine 70). 틀리게 박으면 안 뜬다.
#
# 이 스크립트는 **postgres 유저로** 돈다. 복사하면 새 파일이 자기 소유로 생기므로
# uid 를 알 필요가 없다. 추측을 없애는 게 목적이다.
#
# initdb 때 한 번만 돈다. 복사본은 $PGDATA 에 남으므로(pgdata 볼륨) 재기동에도 유지된다.
set -euo pipefail

cp /certs/server.crt "$PGDATA/server.crt"
cp /certs/server.key "$PGDATA/server.key"
cp /certs/ca.crt "$PGDATA/ca.crt"
chmod 600 "$PGDATA/server.key"
chmod 644 "$PGDATA/server.crt" "$PGDATA/ca.crt"

echo "서버 인증서를 \$PGDATA 로 복사했다 (소유자=$(id -un)):"
ls -l "$PGDATA/server.crt" "$PGDATA/server.key" "$PGDATA/ca.crt"
