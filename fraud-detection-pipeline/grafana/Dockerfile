FROM grafana/grafana:11.5.2

COPY --chown=472:0 grafana/provisioning /etc/grafana/provisioning
COPY --chown=472:0 grafana/dashboards /var/lib/grafana/dashboards

ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/fraud-overview.json
