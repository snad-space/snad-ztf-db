FROM python:3.13

EXPOSE 80

RUN pip install gunicorn

COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt

COPY app /app

HEALTHCHECK --interval=60s --timeout=10s --start-period=80s --retries=3 \
    CMD curl -f http://localhost/api/v1/oid/full/json?oid=830202400008402 && curl -f http://localhost/api/v2/oid/full/json?oid=830202400008402 && curl -f http://localhost/api/v3/data/latest/oid/full/json?oid=830202400008402 || exit 1

ENTRYPOINT ["gunicorn", "-w4", "-b0.0.0.0:80", "--worker-class=aiohttp.GunicornWebWorker", "app.main:get_app"]
