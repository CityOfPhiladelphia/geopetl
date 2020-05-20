
FROM ubuntu:18.04

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux


# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

# Oracle
ENV ORACLE_HOME=/usr/lib/oracle/12.1/client64
ENV LD_LIBRARY_PATH=$ORACLE_HOME/lib
ENV PATH=$ORACLE_HOME/bin:$PATH
ENV HOSTALIASES=/tmp/HOSTALIASES

#RUN set -ex \
#    && apt-get update -yqq \
#    && apt-get install -yqq --no-install-recommends \
#    locales

RUN set -ex \
    && apt-get update -yqq \
    && buildDeps=' \
        python3-dev \
        locales \
    ' \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        alien \
        python3 \
        python3-pip \
        libaio1 \ 
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash worker \
    && pip3 install -U setuptools \
    && pip3 install -U wheel \
    && pip3 install awscli==1.16.140 \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm ./

# instant basic-lite instant oracle client
RUN alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

COPY geopetl /geopetl
COPY setup.py /setup.py

# Upgrade pip stuff so wheel doesn't complain
RUN pip3 install --upgrade pip setuptools wheel
# Install geopetl via setup.py
RUN pip3 install -e .
RUN pip3 install pytest

COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "$POSTGRES_USER", "$POSTGRES_PW", "$POSTGRES_DB"]
