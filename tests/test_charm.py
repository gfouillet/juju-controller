# Copyright 2021 Canonical Ltd.
# Licensed under the GPLv3, see LICENSE file for details.

import ipaddress
import json
import os
import unittest

import yaml

from charms.certificate_transfer_interface.v1.certificate_transfer import (
    ProviderApplicationData,
)
from charms.tempo_coordinator_k8s.v0.tracing import (
    ProtocolType,
    Receiver,
    TracingProviderAppData,
    TransportProtocolType,
)
from charm import JujuControllerCharm, AgentConfException
from ops.model import BlockedStatus, ActiveStatus
from ops.testing import Harness
from unittest.mock import Mock, mock_open, patch
from controlsocket import APIError
from unixsocket import ConnectionError as SocketConnectionError

agent_conf = '''
apiaddresses:
- localhost:17070
cacert: fake
'''

agent_conf_apiaddresses_missing = '''
cacert: fake
'''

agent_conf_apiaddresses_not_list = '''
apiaddresses:
  foo: bar
cacert: fake
'''

agent_conf_ipv4 = '''
apiaddresses:
- "127.0.0.1:17070"
cacert: fake
'''

agent_conf_ipv6 = '''
apiaddresses:
- "[::1]:17070"
cacert: fake
'''


def tracing_provider_data():
    return TracingProviderAppData(
        receivers=[
            Receiver(
                protocol=ProtocolType(name="otlp_grpc", type=TransportProtocolType.grpc),
                url="tempo-grpc:4317",
            ),
            Receiver(
                protocol=ProtocolType(name="otlp_http", type=TransportProtocolType.http),
                url="http://tempo-http:4318",
            ),
        ]
    ).dump()


def certificate_provider_data(certificates):
    return ProviderApplicationData(certificates=certificates).dump()


class TestCharm(unittest.TestCase):
    def setUp(self):
        self.harness = Harness(JujuControllerCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_dashboard_relation_joined(self):
        harness = self.harness

        harness.set_leader(True)
        harness.update_config({"controller-url": "wss://controller/api"})
        harness.update_config({"identity-provider-url": ""})
        harness.update_config({"is-juju": True})
        relation_id = harness.add_relation('dashboard', 'juju-dashboard')
        harness.add_relation_unit(relation_id, 'juju-dashboard/0')

        data = harness.get_relation_data(relation_id, 'juju-controller')
        self.assertEqual(data["controller-url"], "wss://controller/api")
        self.assertEqual(data["is-juju"], 'True')
        self.assertEqual(data.get("identity-provider-url"), None)

    @patch.dict(os.environ, {
        "JUJU_MACHINE_ID": "machine-0",
        "JUJU_UNIT_NAME": "controller/0"
    })
    @patch("ops.model.Model.get_binding")
    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    def test_website_relation_joined(self, _, binding):
        harness = self.harness
        binding.return_value = mockBinding(["192.168.1.17"])

        harness.set_leader()
        relation_id = harness.add_relation('website', 'haproxy')
        harness.add_relation_unit(relation_id, 'haproxy/0')

        data = harness.get_relation_data(relation_id, 'juju-controller/0')
        self.assertEqual(data["hostname"], "192.168.1.17")
        self.assertEqual(data["private-address"], "192.168.1.17")
        self.assertEqual(data["port"], '17070')

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_endpoint_relation(self, mock_remove_user, mock_add_user,
                                       mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)
        harness.add_network(address="192.168.1.17", endpoint="metrics-endpoint")

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_add_user.assert_called_once_with(f'juju-metrics-r{relation_id}', 'passwd')

        mock_metrics_provider.assert_called_once_with(
            harness.charm,
            jobs=[{
                "metrics_path": "/introspection/metrics",
                "scheme": "https",
                "static_configs": [{"targets": ["*:17070"]}],
                "basic_auth": {
                    "username": f'user-juju-metrics-r{relation_id}',
                    "password": 'passwd',
                },
                "tls_config": {
                    "ca_file": 'fake',
                    "server_name": "juju-apiserver",
                },
            }],
        )
        mock_metrics_provider.return_value.set_scrape_job_spec.assert_called_once()

        harness.remove_relation(relation_id)
        mock_remove_user.assert_called_once_with(f'juju-metrics-r{relation_id}')

    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_metrics_endpoint_non_leader_only_sets_unit_data(
            self, mock_add_user, mock_metrics_provider):
        harness = self.harness
        harness.add_relation("metrics-endpoint", "prometheus-k8s")

        mock_add_user.assert_not_called()
        mock_metrics_provider.assert_called_once_with(harness.charm, jobs=[])
        mock_metrics_provider.return_value.set_scrape_job_spec.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_relations_share_user(
            self, mock_remove_user, mock_add_user, _mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        first_id = harness.add_relation("metrics-endpoint", "prometheus-one")
        second_id = harness.add_relation("metrics-endpoint", "prometheus-two")

        # The provider publishes one scrape job to all relations, so separate
        # Prometheus applications intentionally share one controller user.
        username = f"juju-metrics-r{first_id}"
        self.assertEqual(
            harness.get_relation_data(second_id, "juju-controller"),
            {"metrics-username": username, "metrics-password": "passwd"},
        )
        mock_add_user.assert_called_with(username, "passwd")
        mock_remove_user.assert_any_call(f"juju-metrics-r{second_id}")

        mock_remove_user.reset_mock()
        harness.remove_relation(first_id)
        self.assertNotIn(
            username,
            [call.args[0] for call in mock_remove_user.call_args_list],
        )
        mock_remove_user.reset_mock()
        harness.remove_relation(second_id)
        mock_remove_user.assert_any_call(username)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_metrics_endpoint_binding_unresolved_defers_and_retries(
            self, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_provider_instance = mock_metrics_provider.return_value
        mock_provider_instance.set_scrape_job_spec.side_effect = ValueError(
            "'controller-0.controller-service-endpoints.controller-ctrl-xyz.svc.cluster.local' "
            "does not appear to be an IPv4 or IPv6 network"
        )

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_add_user.assert_called_once_with(f'juju-metrics-r{relation_id}', 'passwd')
        self.assertIn(
            f"juju-metrics-r{relation_id}",
            harness.get_relation_data(relation_id, "juju-controller")["metrics-username"],
        )

        notices = [n[0] for n in harness.framework._storage.notices("")]
        self.assertTrue(
            any("metrics_endpoint_relation_created" in n for n in notices)
        )

        mock_provider_instance.update_scrape_job_spec.side_effect = None
        harness.charm._on_metrics_reconcile(None)
        mock_provider_instance.update_scrape_job_spec.assert_called_once()

    @patch("charm.MetricsEndpointProvider", autospec=True)
    def test_metrics_endpoint_non_leader_binding_unresolved_defers(
            self, mock_metrics_provider):
        harness = self.harness
        mock_provider_instance = mock_metrics_provider.return_value
        mock_provider_instance.set_scrape_job_spec.side_effect = ValueError(
            "'controller-0.controller-service-endpoints.controller-ctrl-xyz.svc.cluster.local' "
            "does not appear to be an IPv4 or IPv6 network"
        )

        harness.add_relation("metrics-endpoint", "prometheus-k8s")

        notices = [n[0] for n in harness.framework._storage.notices("")]
        self.assertTrue(
            any("metrics_endpoint_relation_created" in n for n in notices)
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_metrics_endpoint_update_scrape_job_spec_binding_unresolved(
            self, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_provider_instance = mock_metrics_provider.return_value
        harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_provider_instance.set_scrape_job_spec.assert_called_once()

        mock_provider_instance.update_scrape_job_spec.side_effect = ValueError(
            "not an IPv4 or IPv6 network"
        )
        harness.charm._on_metrics_reconcile(None)
        mock_provider_instance.update_scrape_job_spec.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_ensure_user_recovers_from_409(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_add_user.side_effect = [
            APIError({}, 409, "Conflict", "user already exists"),
            None,
        ]

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_remove_user.assert_called_once_with(f'juju-metrics-r{relation_id}')
        self.assertEqual(mock_add_user.call_count, 2)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_ensure_user_recovers_from_500(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_add_user.side_effect = [
            APIError({}, 500, "Internal Server Error",
                     'retrieving existing user "juju-metrics-r1": password destroyed'),
            None,
        ]

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_remove_user.assert_called_once_with(f'juju-metrics-r{relation_id}')
        self.assertEqual(mock_add_user.call_count, 2)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_remove_user_ignores_404(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_remove_user.side_effect = APIError({}, 404, "Not Found", "not found")
        mock_add_user.side_effect = [
            APIError({}, 409, "Conflict", "user already exists"),
            None,
        ]

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        mock_remove_user.assert_called_once_with(f'juju-metrics-r{relation_id}')
        self.assertEqual(mock_add_user.call_count, 2)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_ensure_user_reraises_non_recoverable_error(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_add_user.side_effect = APIError(
            {}, 403, "Forbidden", "forbidden")

        with self.assertRaises(APIError):
            harness.add_relation('metrics-endpoint', 'prometheus-k8s')

        mock_remove_user.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_remove_user_reraises_non_404_error(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_add_user.side_effect = [
            APIError({}, 409, "Conflict", "user already exists"),
            None,
        ]
        mock_remove_user.side_effect = APIError(
            {}, 500, "Internal Server Error", "server error")

        with self.assertRaises(APIError):
            harness.add_relation('metrics-endpoint', 'prometheus-k8s')

        mock_remove_user.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    @patch("controlsocket.ControlSocketClient.remove_metrics_user")
    def test_metrics_ensure_user_retry_failure_propagates(
            self, mock_remove_user, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_add_user.side_effect = [
            APIError({}, 500, "Internal Server Error", "password destroyed"),
            APIError({}, 500, "Internal Server Error", "password destroyed"),
        ]

        with self.assertRaises(APIError):
            harness.add_relation('metrics-endpoint', 'prometheus-k8s')

        self.assertEqual(mock_add_user.call_count, 2)
        mock_remove_user.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password", new=lambda: "passwd")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_metrics_endpoint_redefers_until_resolved(
            self, mock_add_user, mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_provider_instance = mock_metrics_provider.return_value
        mock_provider_instance.set_scrape_job_spec.side_effect = ValueError(
            "not an IPv4 or IPv6 network"
        )
        mock_provider_instance.update_scrape_job_spec.side_effect = ValueError(
            "not an IPv4 or IPv6 network"
        )

        harness.add_relation('metrics-endpoint', 'prometheus-k8s')

        notices = [n[0] for n in harness.framework._storage.notices("")]
        self.assertTrue(
            any("metrics_endpoint_relation_created" in n for n in notices)
        )

        mock_provider_instance.update_scrape_job_spec.side_effect = None
        harness.charm._on_metrics_reconcile(None)
        mock_provider_instance.update_scrape_job_spec.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("charm.MetricsEndpointProvider", autospec=True)
    @patch("charm.generate_password")
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_metrics_endpoint_password_stable_across_deferred_retry(
            self, mock_add_user, mock_generate_password,
            mock_metrics_provider, _):
        harness = self.harness
        harness.set_leader(True)

        mock_generate_password.return_value = "first-pass"

        mock_provider_instance = mock_metrics_provider.return_value
        mock_provider_instance.set_scrape_job_spec.side_effect = ValueError(
            "not an IPv4 or IPv6 network"
        )

        relation_id = harness.add_relation('metrics-endpoint', 'prometheus-k8s')

        self.assertEqual(
            harness.get_relation_data(relation_id, "juju-controller")["metrics-password"],
            "first-pass",
        )

        mock_provider_instance.update_scrape_job_spec.side_effect = None
        mock_generate_password.return_value = "second-pass"
        harness.charm._on_metrics_reconcile(None)

        self.assertEqual(
            harness.get_relation_data(relation_id, "juju-controller")["metrics-password"],
            "first-pass",
        )
        for call in mock_add_user.call_args_list:
            self.assertEqual(call.args[1], "first-pass")

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_tracing_relation_updates_endpoints(self, mock_set_tracing_config, *_):
        harness = self.harness

        relation_id = harness.add_relation("charm-tracing", "tempo-coordinator")
        harness.add_relation_unit(relation_id, "tempo-coordinator/0")

        provider_data = tracing_provider_data()

        harness.update_relation_data(relation_id, "tempo-coordinator", provider_data)

        self.assertEqual(
            harness.charm._stored.tracing_endpoints,
            {"otlp_grpc": "tempo-grpc:4317", "otlp_http": "http://tempo-http:4318"},
        )
        mock_set_tracing_config.assert_called_once_with(
            grpc_endpoint="tempo-grpc:4317",
            http_endpoint="http://tempo-http:4318",
            ca_cert=None,
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_tracing_relation_change_ignores_not_ready(
        self, mock_set_tracing_config, *_
    ):
        harness = self.harness

        event = type("Event", (), {"relation": object()})()
        with patch.object(harness.charm.tracing_requirer, "is_ready", return_value=False):
            harness.charm._on_tracing_relation_changed(event)

        self.assertEqual(harness.charm._stored.tracing_endpoints, {})
        mock_set_tracing_config.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch(
        "controlsocket.ControlSocketClient.set_charm_tracing_config",
        side_effect=SocketConnectionError("could not connect to socket"),
    )
    def test_tracing_relation_update_propagates_socket_error(self, *_):
        harness = self.harness

        relation_id = harness.add_relation("charm-tracing", "tempo-coordinator")
        harness.add_relation_unit(relation_id, "tempo-coordinator/0")

        with self.assertRaisesRegex(
            SocketConnectionError, "could not connect to socket"
        ):
            harness.update_relation_data(
                relation_id, "tempo-coordinator", tracing_provider_data()
            )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_tracing_relation_removed_clears_endpoints(self, mock_set_tracing_config, *_):
        harness = self.harness

        relation_id = harness.add_relation("charm-tracing", "tempo-coordinator")
        harness.add_relation_unit(relation_id, "tempo-coordinator/0")

        harness.update_relation_data(
            relation_id, "tempo-coordinator", tracing_provider_data()
        )
        self.assertEqual(
            harness.charm._stored.tracing_endpoints,
            {"otlp_grpc": "tempo-grpc:4317", "otlp_http": "http://tempo-http:4318"},
        )
        mock_set_tracing_config.assert_called_once_with(
            grpc_endpoint="tempo-grpc:4317",
            http_endpoint="http://tempo-http:4318",
            ca_cert=None,
        )

        harness.remove_relation(relation_id)

        self.assertEqual(harness.charm._stored.tracing_endpoints, {})
        self.assertEqual(mock_set_tracing_config.call_count, 2)
        mock_set_tracing_config.assert_called_with(
            grpc_endpoint=None,
            http_endpoint=None,
            ca_cert=None,
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_receive_ca_cert_updates_stored_ca_cert(self, mock_set_tracing_config, *_):
        harness = self.harness

        relation_id = harness.add_relation("charm-tracing-ca-cert", "cert-provider")
        harness.add_relation_unit(relation_id, "cert-provider/0")

        cert_a = "-----BEGIN CERTIFICATE-----\na\n-----END CERTIFICATE-----"
        cert_b = "-----BEGIN CERTIFICATE-----\nb\n-----END CERTIFICATE-----"
        harness.update_relation_data(
            relation_id,
            "cert-provider",
            certificate_provider_data({cert_b, cert_a}),
        )

        self.assertEqual(harness.charm._stored.ca_cert, "\n".join([cert_a, cert_b]))
        mock_set_tracing_config.assert_called_once_with(
            grpc_endpoint=None,
            http_endpoint=None,
            ca_cert="\n".join([cert_a, cert_b]),
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_receive_ca_cert_update_ignores_empty_cert_list(
        self, mock_set_tracing_config, *_
    ):
        harness = self.harness

        event = type("Event", (), {"certificates": set(), "relation_id": 1})()
        harness.charm._on_receive_ca_cert_updated(event)

        self.assertIsNone(harness.charm._stored.ca_cert)
        mock_set_tracing_config.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("controlsocket.ControlSocketClient.set_charm_tracing_config")
    def test_receive_ca_cert_removed_clears_stored_ca_cert(self, mock_set_tracing_config, *_):
        harness = self.harness

        relation_id = harness.add_relation("charm-tracing-ca-cert", "cert-provider")
        harness.add_relation_unit(relation_id, "cert-provider/0")

        cert = "-----BEGIN CERTIFICATE-----\na\n-----END CERTIFICATE-----"
        harness.update_relation_data(
            relation_id,
            "cert-provider",
            certificate_provider_data({cert}),
        )
        self.assertEqual(harness.charm._stored.ca_cert, cert)
        mock_set_tracing_config.assert_called_once_with(
            grpc_endpoint=None,
            http_endpoint=None,
            ca_cert=cert,
        )

        harness.remove_relation(relation_id)

        self.assertIsNone(harness.charm._stored.ca_cert)
        self.assertEqual(mock_set_tracing_config.call_count, 2)
        mock_set_tracing_config.assert_called_with(
            grpc_endpoint=None,
            http_endpoint=None,
            ca_cert=None,
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf_apiaddresses_missing)
    def test_apiaddresses_missing(self, _):
        harness = self.harness

        with self.assertRaisesRegex(AgentConfException, "agent.conf key 'apiaddresses' missing"):
            harness.charm.api_port()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf_apiaddresses_not_list)
    def test_apiaddresses_not_list(self, _):
        harness = self.harness

        with self.assertRaisesRegex(
            AgentConfException, "agent.conf key 'apiaddresses' is not a list"
        ):
            harness.charm.api_port()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf_apiaddresses_missing)
    @patch("controlsocket.ControlSocketClient.add_metrics_user")
    def test_apiaddresses_missing_status(self, *_):
        harness = self.harness
        harness.set_leader(True)

        harness.add_relation('metrics-endpoint', 'prometheus-k8s')
        harness.evaluate_status()
        self.assertIsInstance(harness.charm.unit.status, BlockedStatus)
        self.assertEqual(
            harness.charm.unit.status,
            BlockedStatus(
                "cannot read controller API port from agent configuration: "
                "agent.conf key 'apiaddresses' missing"
            )
        )

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf_ipv4)
    def test_apiaddresses_ipv4(self, _):
        self.assertEqual(self.harness.charm.api_port(), 17070)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf_ipv6)
    def test_apiaddresses_ipv6(self, _):
        self.assertEqual(self.harness.charm.api_port(), 17070)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_relation_changed_single_addr(
            self, mock_reload_config, mock_get_binding, mock_get_agent_id, *__):
        harness = self.harness
        mock_get_binding.return_value = mockBinding(['192.168.1.17'])

        # This unit's agent ID happens to correspond with the unit ID.
        mock_get_agent_id.return_value = '0'

        harness.set_leader()

        # Have another unit enter the relation.
        # Its bind address should end up in the application data bindings list.
        # Note that the agent ID does not correspond with the unit's ID
        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/1')
        self.harness.update_relation_data(
            relation_id, 'juju-controller/1', {
                'db-bind-address': '192.168.1.100',
                'agent-id': '9',
            })

        mock_reload_config.assert_called_once()

        unit_data = harness.get_relation_data(relation_id, 'juju-controller/0')
        self.assertEqual(unit_data['db-bind-address'], '192.168.1.17')
        self.assertEqual(unit_data['agent-id'], '0')

        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        exp = {'0': '192.168.1.17', '9': '192.168.1.100'}
        self.assertEqual(json.loads(app_data['db-bind-addresses']), exp)

        harness.evaluate_status()
        self.assertIsInstance(harness.charm.unit.status, ActiveStatus)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_relation_changed_multi_addr_error(
            self, mock_reload_config, mock_get_binding, mock_get_agent_id, *_):
        harness = self.harness
        mock_get_binding.return_value = mockBinding(["192.168.1.17", "192.168.1.18"])
        mock_get_agent_id.return_value = '0'

        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/1')

        self.harness.update_relation_data(
            relation_id, 'juju-controller/1', {'db-bind-address': '192.168.1.100'})

        harness.evaluate_status()
        self.assertIsInstance(harness.charm.unit.status, BlockedStatus)
        mock_reload_config.assert_called_once()

    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("builtins.open", new_callable=mock_open)
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_relation_changed_write_file(
            self, mock_reload_config, mock_get_binding, mock_open, mock_get_agent_id):

        harness = self.harness
        mock_get_binding.return_value = mockBinding(['192.168.1.17'])

        mock_get_agent_id.return_value = '0'

        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/1')
        bound = {'juju-controller/0': '192.168.1.17', 'juju-controller/1': '192.168.1.100'}
        self.harness.update_relation_data(
            relation_id, harness.charm.app.name, {'db-bind-addresses': json.dumps(bound)})

        file_path = '/var/lib/juju/agents/controller-0/controller.conf'
        self.assertEqual(mock_open.call_count, 2)

        # First call to read out the YAML
        first_open_args, _ = mock_open.call_args_list[0]
        self.assertEqual(first_open_args, (file_path,))

        # Second call to write the updated YAML.
        second_open_args, _ = mock_open.call_args_list[1]
        self.assertEqual(second_open_args, (file_path, 'w'))

        # yaml.dump appears to write the the file incrementally,
        # so we need to hoover up the call args to reconstruct.
        written = ''
        for args in mock_open().write.call_args_list:
            written += args[0][0]

        self.assertEqual(yaml.safe_load(written), {'db-bind-addresses': bound})

        # The last thing we should have done is send a reload request via the
        # socket..
        mock_reload_config.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_relation_departed(
            self, mock_reload_config, mock_get_binding, mock_get_agent_id, *__):
        harness = self.harness
        mock_get_binding.return_value = mockBinding(['192.168.1.17'])

        # This unit's agent ID happens to correspond with the unit ID.
        mock_get_agent_id.return_value = '0'

        harness.set_leader()

        # Have another unit enter the relation.
        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/1')
        self.harness.update_relation_data(
            relation_id, 'juju-controller/1', {
                'db-bind-address': '192.168.1.100',
                'agent-id': '9',
            })

        # Assert that the second units agent bind address is in the data bag.
        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        exp = {'0': '192.168.1.17', '9': '192.168.1.100'}
        self.assertEqual(json.loads(app_data['db-bind-addresses']), exp)

        # Remove the second unit.
        harness.remove_relation_unit(relation_id, 'juju-controller/1')

        # Assert that the second unit's address is gone from the data bag.
        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        exp = {'0': '192.168.1.17'}
        self.assertEqual(json.loads(app_data['db-bind-addresses']), exp)

        harness.evaluate_status()
        self.assertIsInstance(harness.charm.unit.status, ActiveStatus)

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_relation_departed_ignores_departing_self(
            self, mock_reload_config, mock_get_binding, mock_get_agent_id, *__):
        harness = self.harness
        mock_get_binding.return_value = mockBinding(['192.168.1.17'])
        mock_get_agent_id.return_value = '0'

        harness.set_leader()
        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/1')
        harness.update_relation_data(
            relation_id, 'juju-controller/1', {
                'db-bind-address': '192.168.1.100',
                'agent-id': '9',
            })

        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        expected = {'0': '192.168.1.17', '9': '192.168.1.100'}
        self.assertEqual(json.loads(app_data['db-bind-addresses']), expected)

        mock_reload_config.reset_mock()
        event = Mock(
            relation=harness.model.get_relation('dbcluster', relation_id),
            departing_unit=harness.charm.unit,
        )
        harness.charm._on_dbcluster_relation_departed(event)

        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        self.assertEqual(json.loads(app_data['db-bind-addresses']), expected)
        mock_reload_config.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data=agent_conf)
    @patch("configchangesocket.ConfigChangeSocketClient.get_controller_agent_id")
    @patch("ops.model.Model.get_binding")
    @patch("configchangesocket.ConfigChangeSocketClient.reload_config")
    def test_dbcluster_leader_elected_reconciles_bind_addresses(
            self, mock_reload_config, mock_get_binding, mock_get_agent_id, *__):
        harness = self.harness
        mock_get_binding.return_value = mockBinding(['192.168.1.17'])
        mock_get_agent_id.return_value = '1'

        relation_id = harness.add_relation('dbcluster', harness.charm.app.name)
        harness.add_relation_unit(relation_id, 'juju-controller/2')
        harness.update_relation_data(
            relation_id, 'juju-controller/2', {
                'db-bind-address': '192.168.1.100',
                'agent-id': '2',
            })
        stale = {
            '0': '192.168.1.16',
            '1': '192.168.1.17',
            '2': '192.168.1.100',
        }
        harness.update_relation_data(
            relation_id,
            harness.charm.app.name,
            {'db-bind-addresses': json.dumps(stale)},
        )

        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        self.assertEqual(json.loads(app_data['db-bind-addresses']), stale)

        mock_reload_config.reset_mock()
        harness.set_leader()

        app_data = harness.get_relation_data(relation_id, 'juju-controller')
        expected = {'1': '192.168.1.17', '2': '192.168.1.100'}
        self.assertEqual(json.loads(app_data['db-bind-addresses']), expected)
        mock_reload_config.assert_called_once()


class mockNetwork:
    def __init__(self, addresses):
        self.ingress_addresses = [ipaddress.ip_address(addr) for addr in addresses]
        self.ingress_address = addresses[0]


class mockBinding:
    def __init__(self, addresses):
        self.network = mockNetwork(addresses)
