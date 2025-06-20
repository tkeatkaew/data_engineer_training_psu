FROM debian:sid

# comment this line if you are outside PSU
RUN echo 'deb http://mirror.psu.ac.th/debian/ sid main contrib non-free non-free-firmware' > /etc/apt/sources.list



RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-wheel \
    python3-venv \
    sudo \
    wget \
    openssh-server

RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.12.2.deb && \
    apt install ./mongodb-database-tools-*.deb && \
    rm -f mongodb-database-tools-*.deb

RUN useradd -m -s /bin/bash -d /home/psu psu
RUN usermod -aG sudo psu
RUN echo 'psu:coe' | chpasswd

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["sh", "/docker-entrypoint.sh"]

RUN python3 -m venv /venv

RUN . /venv/bin/activate && pip install poetry gunicorn

WORKDIR /app
COPY code /app

RUN . /venv/bin/activate \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --only main


# USER psu