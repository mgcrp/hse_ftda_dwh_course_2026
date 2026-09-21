FROM postgres:18

USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-venv ca-certificates curl; \
    python3 -m venv /opt/patroni; \
    /opt/patroni/bin/pip install --no-cache-dir --upgrade pip; \
    /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]==4.1.5" "psycopg[binary]"; \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/patroni/bin:/usr/lib/postgresql/18/bin:${PATH}"
ENV PATRONICTL_CONFIG_FILE=/etc/patroni.yml

COPY patroni.yml /etc/patroni.yml
RUN chmod 0644 /etc/patroni.yml

USER postgres
ENTRYPOINT ["patroni", "/etc/patroni.yml"]
