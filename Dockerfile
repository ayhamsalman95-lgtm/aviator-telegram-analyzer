FROM python:3.13-bookworm

ENV PYTHONUNBUFFERED=1     PYTHONDONTWRITEBYTECODE=1     DISPLAY=:100

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y --no-install-recommends     chromium     xvfb     x11vnc     fluxbox     novnc     fonts-noto-core     ca-certificates     && rm -rf /var/lib/apt/lists/*     && chmod +x /app/start_railway.sh

RUN pip install --no-cache-dir -r requirements.txt

CMD ["/app/start_railway.sh"]
