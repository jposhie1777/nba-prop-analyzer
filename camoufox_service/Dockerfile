# Pin to Bookworm (Debian 12) explicitly.
# python:3.11-slim now resolves to Trixie (Debian 13) on some architectures,
# where several package names changed (libgdk-pixbuf2.0-0 was removed, etc.).
FROM python:3.11-slim-bookworm

# System dependencies required by Camoufox/Firefox on Debian Bookworm.
# These mirror the apt-get installs used in the existing GitHub Actions
# Camoufox workflows (epl_oddspedia_loader.yml, oddspedia_mls_odds_daily.yml).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libxt6 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    libasound2 \
    libpangocairo-1.0-0 \
    libatk1.0-0 \
    libcairo-gobject2 \
    libgdk-pixbuf2.0-0 \
    fonts-liberation \
    ca-certificates \
    xvfb \
    xauth \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-fetch the Camoufox Firefox binary at build time so startup is instant.
# GITHUB_TOKEN avoids GitHub API rate-limit errors during the binary download.
ARG GITHUB_TOKEN
RUN GITHUB_TOKEN=${GITHUB_TOKEN} python -m camoufox fetch

COPY . .

RUN chmod +x /app/start.sh

ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99

# start.sh launches Xvfb then uvicorn.
# Firefox's GTK libraries require a display socket even with headless=True;
# Xvfb provides a virtual one so no real screen is needed.
CMD ["/app/start.sh"]
