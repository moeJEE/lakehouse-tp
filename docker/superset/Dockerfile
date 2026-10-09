ARG SUPERSET_VERSION=4.1.2
FROM apache/superset:${SUPERSET_VERSION}
USER root
# Pilote SQLAlchemy pour Trino (non inclus dans l'image officielle)
RUN pip install --no-cache-dir "trino[sqlalchemy]==0.330.0"
COPY superset-init.sh /app/superset-init.sh
RUN chmod +x /app/superset-init.sh
USER superset
CMD ["/app/superset-init.sh"]
