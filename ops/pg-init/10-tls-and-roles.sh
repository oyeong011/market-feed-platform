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
#
# **끝에 덧붙이면 안 된다.** pg_hba 는 위에서 아래로 첫 일치를 쓴다. 이미지 기본값에는
# `host all all all scram-sha-256` 이 이미 있으므로, 뒤에 붙인 규칙은 영원히 안 읽힌다 —
# 설정은 들어 있는데 동작은 안 바뀌는, 이 저장소가 계속 만난 유형이다.
# 그래서 원격(host*) 규칙을 **걷어내고** 우리 것만 남긴다. local 줄은 남긴다 —
# initdb 스크립트와 healthcheck 가 유닉스 소켓으로 붙는다.
grep -vE '^[[:space:]]*(host|hostssl|hostnossl)[[:space:]]' "$HBA" > "$HBA.new"
{
  echo ""
  echo "# mdfeed: 원격 접속은 TLS + 클라이언트 인증서만. 비밀번호 인증은 걷어냈다."
  echo "# ops/pg-init/10-tls-and-roles.sh 가 넣는다. 역할은 비밀번호가 없다(bootstrap_roles.sql)."
  echo "hostssl all all all cert clientcert=verify-full"
} >> "$HBA.new"
mv "$HBA.new" "$HBA"
chmod 600 "$HBA"

echo "pg_hba: 원격 규칙을 hostssl cert 로 교체했다"
grep -vE '^[[:space:]]*#|^[[:space:]]*$' "$HBA" || true

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/bootstrap_roles.sql

echo "역할 생성 완료:"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -Atc \
  "SELECT rolname FROM pg_roles WHERE rolname LIKE 'mdfeed%' ORDER BY 1"
