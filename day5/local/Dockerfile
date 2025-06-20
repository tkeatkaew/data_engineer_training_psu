FROM debian:sid

# comment this line if you are outside PSU
RUN echo 'deb http://mirror.psu.ac.th/debian/ sid main contrib non-free non-free-firmware' > /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    git \
    sudo \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-venv \
    openssh-client \
    vim \
    iputils-ping \
    ansible 

RUN useradd -m -s /bin/bash -d /home/psu psu && \
    echo "psu:coe" | chpasswd && \
    usermod -aG sudo psu
# USER psu

WORKDIR /home/psu


CMD ["tail", "-f", "/dev/null"]
