FROM python:3.8

EXPOSE 80

RUN pip install gunicorn

COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt

COPY app /app

ENTRYPOINT ["gunicorn", "-w4", "-b0.0.0.0:80", "--worker-class=aiohttp.GunicornWebWorker", "app.main:get_app"]
