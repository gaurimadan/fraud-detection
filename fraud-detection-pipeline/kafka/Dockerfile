FROM apache/kafka:3.9.1

USER root
COPY kafka/render-entrypoint.sh /usr/local/bin/render-entrypoint.sh
RUN chmod +x /usr/local/bin/render-entrypoint.sh
USER appuser

ENTRYPOINT ["/usr/local/bin/render-entrypoint.sh"]
CMD ["/etc/kafka/docker/run"]
