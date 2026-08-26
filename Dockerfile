# MDFeed 컨테이너 이미지
#
# 핵심 경로가 표준 라이브러리만 쓰므로 python:slim 위에 소스만 올리면 끝난다.
# 빌드 도구·컴파일러가 필요 없어 이미지가 작고 CVE 표면도 좁다.

FROM python:3.12-slim

# 보안: 루트로 돌리지 않는다
RUN useradd --system --create-home --home-dir /home/mdfeed --shell /usr/sbin/nologin mdfeed

WORKDIR /opt/mdfeed

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

# 서비스마다 자기 포트를 검사하도록 compose 에서 덮어쓴다.
# 기본값은 feedd 기준.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import urllib.request,json,sys; \
      d=json.loads(urllib.request.urlopen('http://127.0.0.1:9100/healthz',timeout=4).read()); \
      sys.exit(0 if d.get('healthy') else 1)"

CMD ["python3", "-m", "mdfeed.services.feedd"]
