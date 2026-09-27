#!/bin/bash
#
# initdb 직후에 도는 스크립트. 여기서 하는 일이 둘이다.
#
# 1. pg_hba 에 인증서 인증 규칙을 넣는다. 이미지 기본값은 비밀번호 인증이므로,
#    인증서만 있는 역할(ops/bootstrap_roles.sql 이 만드는 것들)은 못 붙는다.
# 2. 역할을 만든다. bootstrap_roles.sql 은 저장소에 있었지만 **아무도 실행하지
#    않았다** — compose 는 initdb 디렉터리에 넣지 않았고, 그래서 mdfeed_runtime 이
#    존재하지 않는 DB 에 DSN 이 그 역할로 접속하려 하고 있었다(결함 48).
#
# 이 스크립트는 entrypoint 가 띄운 임시 서버에 대해 돈다. pg_hba 변경은 그 뒤
# 최종 기동에 반영된다.
set -euo pipefail

HBA="$PGDATA/pg_hba.conf"

# hostssl + cert: TLS 를 강제하고, 클라이언트 인증서의 CN 을 역할명으로 본다.
# clientcert=verify-full 은 CN 이 접속 역할과 같아야 한다는 뜻이다 — 인증서 하나를
# 훔쳐도 그 역할로만 쓸 수 있다.
{
  echo ""
  echo "# mdfeed: 인증서 인증. ops/pg-init/10-tls-and-roles.sh 가 넣는다."
  echo "hostssl all all all cert clientcert=verify-full"
} >> "$HBA"

echo "pg_hba 에 hostssl cert 규칙 추가"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/bootstrap_roles.sql

echo "역할 생성 완료:"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -Atc \
  "SELECT rolname FROM pg_roles WHERE rolname LIKE 'mdfeed%' ORDER BY 1"
