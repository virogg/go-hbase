# T26: slim HBase 2.5 standalone dev image.
#
# We build from a pinned tarball (verified by sha512) rather than relying on
# third-party images, because no public image ships HBase 2.5.x with a stable
# tag we trust. Standalone mode runs Master + RegionServer + embedded ZK in
# a single JVM, which is enough for coproc integration tests.

FROM eclipse-temurin:11-jdk-jammy

ARG HBASE_VERSION=2.5.11
ARG HBASE_SHA512=BAD94B693C2F3013F91FB50BF8678D48C749815A8CA335531E85C0B62BFC18D34DF253A43B50AA66B7ED5C1FF379E8EA0213B4A6F21441F93DD83EB21DEC473C
# Default to archive.apache.org for reproducible builds; override at build time
# (`docker build --build-arg HBASE_DIST_BASE_URL=https://dlcdn.apache.org/hbase`)
# to use a faster CDN mirror when the pinned version is still current.
ARG HBASE_DIST_BASE_URL=https://archive.apache.org/dist/hbase

ENV HBASE_HOME=/opt/hbase
ENV PATH=${HBASE_HOME}/bin:${PATH}

# curl: tarball fetch + container healthcheck. procps: jps/ps for HBase shell.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    curl -fsSL "${HBASE_DIST_BASE_URL}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" \
         -o /tmp/hbase.tar.gz; \
    actual=$(sha512sum /tmp/hbase.tar.gz | awk '{print toupper($1)}'); \
    expected=$(printf '%s' "${HBASE_SHA512}" | tr -d ' \n' | tr '[:lower:]' '[:upper:]'); \
    if [ "${actual}" != "${expected}" ]; then \
        echo "sha512 mismatch: got ${actual}, expected ${expected}" >&2; exit 1; \
    fi; \
    mkdir -p "${HBASE_HOME}"; \
    tar -xzf /tmp/hbase.tar.gz -C "${HBASE_HOME}" --strip-components=1; \
    rm /tmp/hbase.tar.gz

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY hbase-env.sh ${HBASE_HOME}/conf/hbase-env.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 2181 16000 16010 16020 16030

ENTRYPOINT ["/entrypoint.sh"]
