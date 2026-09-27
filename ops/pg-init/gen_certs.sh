#!/bin/sh
#
# Postgres TLS 인증서를 **볼륨 안에서** 만든다.
#
# 호스트에서 만들어 바인드 마운트하면 안 된다. Postgres 는 서버 키가 자기 소유이고
# 0600 이 아니면 기동을 거부한다("private key file has group or world access").
# 호스트 파일의 uid/mode 는 컨테이너로 그대로 넘어가므로, 기계마다 다른 값에
# 기동 여부가 걸린다.
#
# 소유권은 맞춰야 하는데 postgres uid 는 이미지마다 다르다(debian 999 · alpine 70).
# 그래서 이 스크립트를 **postgres 이미지 안에서** 돌린다 — 그러면 uid 를 몰라도
# `chown postgres:postgres` 라고 이름으로 쓸 수 있다.
#
# initdb 스크립트로 $PGDATA 에 복사하는 방법을 먼저 시도했는데 안 된다. entrypoint 가
# 띄우는 **임시 서버도 command 인자를 적용**하므로, initdb 스크립트가 돌기 전에
# ssl_cert_file 이 이미 있어야 한다. 없으면 거기서 FATAL 로 죽는다.
#
# root 소유 0640 도 안 된다. Postgres 는 root 소유일 때 0640 까지 허용하지만,
# 그러면 그룹이 root 라서 postgres 유저가 **읽지 못한다.** 0644 로 올리면
# 권한 검사에서 거부된다. 결국 소유자를 맞추는 수밖에 없다.
#
# 클라이언트 키는 읽는 uid 를 고정한다(Dockerfile 의 mdfeed uid 10001).
#
# 클라이언트 인증서도 같이 만든다. ops/bootstrap_roles.sql 은 역할을 비밀번호 없이
# 만들고(그게 의도이고 시험이 그걸 고정한다), 그러면 남는 인증 수단은 인증서다.
set -eu

CERTS=/certs
CLIENT_UID=${CLIENT_UID:-10001}   # Dockerfile 의 mdfeed uid
SERVER_CN=${SERVER_CN:-postgres}
CLIENT_CN=${CLIENT_CN:-mdfeed_runtime}
DAYS=${DAYS:-825}

if [ -f "$CERTS/server.key" ] && [ -f "$CERTS/ca.crt" ] && [ -f "$CERTS/client.key" ]; then
  echo "인증서가 이미 있다 — 그대로 쓴다"
  exit 0
fi

mkdir -p "$CERTS"
cd "$CERTS"

echo "CA 생성"
openssl req -new -x509 -nodes -newkey rsa:2048 -days "$DAYS" \
  -keyout ca.key -out ca.crt -subj "/CN=mdfeed-dev-ca" >/dev/null 2>&1

echo "서버 인증서 생성 (CN=$SERVER_CN)"
openssl req -new -nodes -newkey rsa:2048 -keyout server.key -out server.csr \
  -subj "/CN=$SERVER_CN" >/dev/null 2>&1
# SAN 이 없으면 sslmode=verify-full 이 호스트명 검증에서 떨어진다.
# verify-ca 는 통과하는데 verify-full 만 떨어지므로 증상이 헷갈린다.
printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\n' "$SERVER_CN" > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -extfile server.ext >/dev/null 2>&1

echo "클라이언트 인증서 생성 (CN=$CLIENT_CN — 이 CN 이 곧 접속 역할명이다)"
openssl req -new -nodes -newkey rsa:2048 -keyout client.key -out client.csr \
  -subj "/CN=$CLIENT_CN" >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS" >/dev/null 2>&1

rm -f server.csr client.csr server.ext ca.srl

# 서버 키: postgres 소유 0600. 이름으로 chown 하므로 uid 를 몰라도 된다.
chown postgres:postgres server.key server.crt ca.crt
chmod 600 server.key
chmod 644 server.crt ca.crt

# 클라이언트 키: libpq 는 "현재 uid 소유 + 0600" 또는 "root 소유 + 0640 이하" 만 받는다.
# 이미지 uid 를 고정했으므로 전자로 맞춘다.
chown "$CLIENT_UID:$CLIENT_UID" client.key client.crt
chmod 600 client.key
chmod 644 client.crt

echo "완료:"
ls -l "$CERTS"
