# MDFeed 컨테이너 이미지
#
# 핵심 경로가 표준 라이브러리만 쓰므로 python:slim 위에 소스만 올리면 끝난다.
# 빌드 도구·컴파일러가 필요 없어 이미지가 작고 CVE 표면도 좁다.

FROM python:3.12-slim

# 보안: 루트로 돌리지 않는다
RUN useradd --system --create-home --home-dir /home/mdfeed --shell /usr/sbin/nologin mdfeed

WORKDIR /opt/mdfeed

# 핵심 경로는 표준 라이브러리만 쓰지만, compose 스택은 PostgreSQL 을 함께 띄운다.
# 드라이버가 없으면 DATABASE_URL 을 줘도 조용히 SQLite 로 떨어진다 — 실제로 그랬다.
RUN pip install --no-cache-dir psycopg2-binary==2.9.9

# 헬스체크에 curl 대신 파이썬을 쓰므로 추가 패키지가 없다
COPY src/ ./src/
COPY quant/ ./quant/
COPY bench/ ./bench/
COPY ops/ ./ops/
COPY docs/ ./docs/
COPY Makefile pyproject.toml ./

RUN mkdir -p /var/lib/mdfeed /run/mdfeed /data \
 && chown -R mdfeed:mdfeed /opt/mdfeed /var/lib/mdfeed /run/mdfeed /data

ENV PYTHONPATH=/opt/mdfeed/src \
    PYTHONUNBUFFERED=1 \
    MDFEED_RUN_DIR=/run/mdfeed \
    MDFEED_SQLITE_PATH=/var/lib/mdfeed/mdfeed.db \
    MDFEED_HTTP_HOST=0.0.0.0 \
    MDFEED_LOG_JSON=1

USER mdfeed

# feedd:9100  tcp:9101  ws/대시보드:9102  rest:9103  writer:9104  strategy:9105  tcp-admin:9111
EXPOSE 9100 9101 9102 9103 9104 9105 9111

# 헬스체크 포트는 서비스마다 다르다. 이미지에 9100 을 박아두면 tcp-gateway 컨테이너는
# 있지도 않은 포트를 검사해 영원히 unhealthy 가 된다 — 실제로 그랬다.
# MDFEED_HEALTH_PORT 로 컨테이너마다 지정하고, 기본값만 feedd 로 둔다.
ENV MDFEED_HEALTH_PORT=9100
HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=3 \
  CMD python3 -c "import os,urllib.request,json,sys; \
      p=os.getenv('MDFEED_HEALTH_PORT','9100'); \
      d=json.loads(urllib.request.urlopen(f'http://127.0.0.1:{p}/healthz',timeout=4).read()); \
      sys.exit(0 if d.get('healthy') else 1)"

CMD ["python3", "-m", "mdfeed.services.feedd"]
