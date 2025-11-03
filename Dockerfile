FROM alpine:3.22

RUN apk add --no-cache openssh
RUN adduser -D -h /home/mhuttner mhuttner \
    && mkdir -p /home/mhuttner/.ssh \
    && chmod 700 /home/mhuttner/.ssh \
    && chown -R mhuttner:mhuttner /home/mhuttner \
    && passwd -u mhuttner

ARG PUBKEY
RUN echo "$PUBKEY" > /home/mhuttner/.ssh/authorized_keys && \
    chmod 600 /home/mhuttner/.ssh/authorized_keys && \
    chown mhuttner:mhuttner /home/mhuttner/.ssh/authorized_keys

RUN ssh-keygen -A && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config

CMD ["/usr/sbin/sshd", "-D", "-e"]
