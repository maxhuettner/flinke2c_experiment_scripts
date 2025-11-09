FROM ubuntu:noble

RUN apt-get update && \
    apt-get install -y openssh-server && \
    apt-get clean

RUN useradd \
      --home-dir /home/mhuttner \
      --create-home \
      --shell /bin/bash \
      mhuttner \
    && mkdir -p /home/mhuttner/.ssh \
    && chmod 700 /home/mhuttner/.ssh \
    && chown -R mhuttner:mhuttner /home/mhuttner

ARG PUBKEY
RUN echo "$PUBKEY" > /home/mhuttner/.ssh/authorized_keys && \
    chmod 600 /home/mhuttner/.ssh/authorized_keys && \
    chown mhuttner:mhuttner /home/mhuttner/.ssh/authorized_keys

RUN mkdir -p /run/sshd && chmod 755 /run/sshd

RUN ssh-keygen -A && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config

CMD ["/usr/sbin/sshd", "-D", "-e"]
