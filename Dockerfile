# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS slackdump

ARG TARGETARCH
ARG SLACKDUMP_VERSION=4.4.5

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "$TARGETARCH" in \
         amd64) archive_arch=x86_64; expected_sha=27056c717d7d0142a3e5bc95cf49d13a60c84b3da0d9af9f89715d26dd59a272 ;; \
         arm64) archive_arch=arm64; expected_sha=7539de4287346d0b3061930c25f8fd3a643725bcc6a162ed91b2f4638bbaf999 ;; \
         *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && curl -fsSLo /tmp/slackdump.tar.gz "https://github.com/rusq/slackdump/releases/download/v${SLACKDUMP_VERSION}/slackdump_Linux_${archive_arch}.tar.gz" \
    && printf '%s  %s\n' "$expected_sha" /tmp/slackdump.tar.gz | sha256sum -c - \
    && tar -xzf /tmp/slackdump.tar.gz -C /usr/local/bin slackdump \
    && chmod 0755 /usr/local/bin/slackdump

FROM python:3.12-slim AS build

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip wheel --disable-pip-version-check --wheel-dir /wheels ".[deployment]"

FROM python:3.12-slim AS runtime

ENV DAGSTER_HOME=/opt/slackpipe/deployment \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --gid 10001 slackpipe \
    && useradd --uid 10001 --gid slackpipe --create-home --home-dir /home/slackpipe slackpipe \
    && mkdir -p /opt/slackpipe/deployment /var/lib/dagster /var/lib/slackpipe \
    && chown -R slackpipe:slackpipe /var/lib/dagster /var/lib/slackpipe

COPY --from=build /wheels /wheels
RUN python -m pip install --disable-pip-version-check --no-cache-dir --no-index --find-links=/wheels "slackpipe[deployment]" \
    && rm -rf /wheels
COPY --from=slackdump /usr/local/bin/slackdump /usr/local/bin/slackdump
COPY deployment/dagster.yaml deployment/workspace.yaml deployment/reload_watch.py /opt/slackpipe/deployment/

WORKDIR /opt/slackpipe
USER slackpipe

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/slackpipe/deployment/workspace.yaml"]
