#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# Licensed under the GPLv3, see LICENSE file for details.

import controlsocket
import json
import logging
import secrets
import urllib.parse
import yaml
import ops
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from typing import List

logger = logging.getLogger(__name__)


class JujuControllerCharm(ops.CharmBase):
    METRICS_USERNAME_KEY = "metrics-username"
    METRICS_PASSWORD_KEY = "metrics-password"

    _stored = ops.StoredState()

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.leader_elected, self._on_metrics_reconcile)
        self.framework.observe(self.on.upgrade_charm, self._on_metrics_reconcile)
        self.framework.observe(
            self.on.dashboard_relation_joined, self._on_dashboard_relation_joined)
        self.framework.observe(
            self.on.website_relation_joined, self._on_website_relation_joined)

        self.control_socket = controlsocket.Client(
            socket_path="/var/lib/juju/control.socket")
        self.framework.observe(
            self.on.metrics_endpoint_relation_created, self._on_metrics_endpoint_relation_created)
        self.framework.observe(
            self.on.metrics_endpoint_relation_changed, self._on_metrics_reconcile)
        self.framework.observe(
            self.on.metrics_endpoint_relation_broken, self._on_metrics_endpoint_relation_broken)
        self._metrics_endpoint = None

    def _on_start(self, _):
        self.unit.status = ops.ActiveStatus()

    def _on_config_changed(self, _):
        controller_url = self.config["controller-url"]
        logger.info("got a new controller-url: %r", controller_url)

    def _on_dashboard_relation_joined(self, event):
        logger.info("got a new dashboard relation: %r", event)
        if self.unit.is_leader():
            event.relation.data[self.app].update({
                'controller-url': self.config['controller-url'],
                'identity-provider-url': self.config['identity-provider-url'],
                'is-juju': str(self.config['is-juju']),
            })
        # TODO: do we need to poke something on the controller so that the `juju
        # dashboard` command will work?

    def _on_website_relation_joined(self, event):
        """Connect a website relation."""
        logger.info("got a new website relation: %r", event)
        port = self.api_port()
        if port is None:
            logger.error("machine does not appear to be a controller")
            self.unit.status = ops.BlockedStatus('machine does not appear to be a controller')
            return

        address = None
        binding = self.model.get_binding(event.relation)
        if binding:
            address = binding.network.ingress_address
            if self.unit.is_leader():
                event.relation.data[self.unit].update({
                    'hostname': str(address),
                    'private-address': str(address),
                    'port': str(port)
                })

    def _metrics_credentials(self, relations):
        for relation in relations:
            data = relation.data[self.app]
            username = data.get(self.METRICS_USERNAME_KEY)
            password = data.get(self.METRICS_PASSWORD_KEY)
            if username and password:
                return username, password

            try:
                jobs = json.loads(data.get("scrape_jobs", "[]"))
                basic_auth = jobs[0]["basic_auth"]
                username = basic_auth["username"]
                # Use removeprefix once Python 3.8 support is dropped.
                if username.startswith("user-"):
                    username = username[len("user-"):]
                return username, basic_auth["password"]
            except (IndexError, KeyError, TypeError, ValueError):
                continue
        return None

    def _metrics_jobs(self, username, password):
        try:
            api_port = self.api_port()
        except AgentConfException as e:
            self.unit.status = ops.BlockedStatus(
                f"can't read controller API port from agent.conf: {e}")
            return None
        return [{
            "metrics_path": "/introspection/metrics",
            "scheme": "https",
            "static_configs": [{"targets": [f"*:{api_port}"]}],
            "basic_auth": {
                "username": f"user-{username}",
                "password": password,
            },
            "tls_config": {
                "ca_file": self.ca_cert(),
                "server_name": "juju-apiserver",
            },
        }]

    def _configure_metrics_endpoint(self, username, password):
        jobs = self._metrics_jobs(username, password)
        if jobs is None:
            return
        if self._metrics_endpoint is None:
            self._metrics_endpoint = MetricsEndpointProvider(self, jobs=jobs)
            self._metrics_endpoint.set_scrape_job_spec()
        else:
            self._metrics_endpoint.update_scrape_job_spec(jobs)

    def _configure_metrics_as_unit(self):
        if self._metrics_endpoint is None:
            self._metrics_endpoint = MetricsEndpointProvider(self, jobs=[])
        self._metrics_endpoint.set_scrape_job_spec()

    def _remove_metrics_user(self, username):
        try:
            self.control_socket.remove_metrics_user(username)
        except controlsocket.APIError as e:
            if e.code != 404:
                raise

    def _ensure_metrics_user(self, username, password):
        try:
            self.control_socket.add_metrics_user(username, password)
        except controlsocket.APIError as e:
            if e.code != 409:
                raise
            self._remove_metrics_user(username)
            self.control_socket.add_metrics_user(username, password)

    def _reconcile_metrics_as_leader(self, relations):
        credentials = self._metrics_credentials(relations)
        if credentials is None:
            # MetricsEndpointProvider publishes one scrape job to every
            # metrics-endpoint relation, so all Prometheus applications share
            # one controller user. The oldest relation only seeds its name.
            username = metrics_username(min(relations, key=lambda r: r.id))
            password = generate_password()
        else:
            username, password = credentials

        for relation in relations:
            relation.data[self.app].update({
                self.METRICS_USERNAME_KEY: username,
                self.METRICS_PASSWORD_KEY: password,
            })
        self._ensure_metrics_user(username, password)
        for relation in relations:
            old_username = metrics_username(relation)
            if old_username != username:
                self._remove_metrics_user(old_username)
        self._configure_metrics_endpoint(username, password)

    def _reconcile_metrics(self, relations):
        if not relations:
            return False
        if self.unit.is_leader():
            self._reconcile_metrics_as_leader(relations)
        else:
            self._configure_metrics_as_unit()
        return True

    def _on_metrics_endpoint_relation_created(self, event):
        relations = self.model.relations["metrics-endpoint"]
        if not self._reconcile_metrics(relations):
            event.defer()

    def _on_metrics_reconcile(self, _event):
        self._reconcile_metrics(self.model.relations["metrics-endpoint"])

    def _on_metrics_endpoint_relation_broken(self, event):
        relations = [
            relation for relation in self.model.relations["metrics-endpoint"]
            if relation.id != event.relation.id
        ]
        credentials = self._metrics_credentials(
            [event.relation] + relations
        )
        if relations:
            self._reconcile_metrics(relations)
            if self.unit.is_leader() and credentials:
                old_username = metrics_username(event.relation)
                if old_username != credentials[0]:
                    self._remove_metrics_user(old_username)
            return

        if not self.unit.is_leader():
            return
        usernames = {metrics_username(event.relation)}
        if credentials:
            usernames.add(credentials[0])
        for username in usernames:
            self._remove_metrics_user(username)

    def _agent_conf(self, key: str):
        """Read a value (by key) from the agent.conf file on disk."""
        unit_name = self.unit.name.replace('/', '-')
        agent_conf_path = f'/var/lib/juju/agents/unit-{unit_name}/agent.conf'

        with open(agent_conf_path) as agent_conf_file:
            agent_conf = yaml.safe_load(agent_conf_file)
            return agent_conf.get(key)

    def api_port(self) -> str:
        """Return the port on which the controller API server is listening."""
        api_addresses = self._agent_conf('apiaddresses')
        if not api_addresses:
            raise AgentConfException("agent.conf key 'apiaddresses' missing")
        if not isinstance(api_addresses, List):
            raise AgentConfException("agent.conf key 'apiaddresses' is not a list")

        parsed_url = urllib.parse.urlsplit('//' + api_addresses[0])
        if not parsed_url.port:
            raise AgentConfException("api address doesn't include port")
        return parsed_url.port

    def ca_cert(self) -> str:
        """Return the controller's CA certificate."""
        return self._agent_conf('cacert')


def metrics_username(relation: ops.Relation) -> str:
    """
    Return the username used to access the metrics endpoint, for the given
    relation. This username has the form
        juju-metrics-r1
    """
    return f'juju-metrics-r{relation.id}'


def generate_password() -> str:
    return secrets.token_urlsafe(16)


class AgentConfException(Exception):
    """Raised when there are errors reading info from agent.conf."""


if __name__ == "__main__":
    ops.main(JujuControllerCharm)
