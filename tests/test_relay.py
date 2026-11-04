# SPDX-License-Identifier: GPL-3.0-or-later
import sys, types
sys.path.insert(0, sys.argv[1])
import relay as R
from meshtastic.protobuf import module_config_pb2, channel_pb2, config_pb2, mesh_pb2, localonly_pb2
from pubsub import pub

calls = {"pub": [], "sub": [], "radio": []}
class FakeRC:
    is_failure = False
class FakeClient:
    def __init__(self, *a, **k): self.k = k
    def username_pw_set(self, u, p): calls["auth"] = (bool(u), bool(p))
    def tls_set_context(self, c): calls["tls"] = True
    def max_queued_messages_set(self, n): pass
    def reconnect_delay_set(self, **k): pass
    def connect_async(self, h, p, keepalive): calls["connect"] = (h, p)
    def loop_start(self): self.on_connect(self, None, {}, FakeRC(), None)
    def loop_stop(self): pass
    def disconnect(self): pass
    def subscribe(self, t): calls["sub"] = t
    def publish(self, topic, payload, qos, retain):
        calls["pub"].append((topic, payload, retain)); return types.SimpleNamespace(rc=0)
R.mqtt.Client = FakeClient

def make_iface(name="TA_Mesh", preset_only=False):
    node = types.SimpleNamespace()
    node.moduleConfig = module_config_pb2.ModuleConfig()
    m = node.moduleConfig.mqtt
    m.enabled = True; m.proxy_to_client_enabled = True; m.address = "mqtt.tamesh.org"
    m.username = "u"; m.password = "p"; m.root = "msh"
    node.localConfig = localonly_pb2.LocalConfig(); node.localConfig.lora.use_preset = True
    node.localConfig.lora.modem_preset = config_pb2.Config.LoRaConfig.ModemPreset.LONG_FAST
    ch0 = channel_pb2.Channel(index=0, role=channel_pb2.Channel.Role.PRIMARY)
    ch0.settings.name = "" if preset_only else name; ch0.settings.downlink_enabled = True; ch0.settings.uplink_enabled = True
    ch1 = channel_pb2.Channel(index=1, role=channel_pb2.Channel.Role.DISABLED)
    node.channels = [ch0, ch1]
    iface = types.SimpleNamespace(localNode=node, myInfo=types.SimpleNamespace(my_node_num=0x12345678))
    iface.sendMqttClientProxyMessage = lambda topic, data: calls["radio"].append((topic, data))
    return iface

s = R.read_settings(make_iface()); print("settings", s.host, s.port, s.topics, s.gateway_id, "repr hides secrets:", "p'" not in repr(s))
print("preset fallback", R.read_settings(make_iface(preset_only=True)).topics)
r = R.MqttProxyRelay(); r.start(); iface = make_iface(); r.attach(iface)
print("status", r.status, "connect", calls["connect"], "sub", calls["sub"], "auth", calls["auth"])
# uplink via pubsub, exactly as the meshtastic library publishes it
pm = mesh_pb2.MqttClientProxyMessage(topic="msh/2/e/TA_Mesh/!12345678", data=b"\x01\x02", retained=False)
pub.sendMessage(R.PROXY_TOPIC, proxymessage=pm, interface=iface)
print("uplink", calls["pub"], r.uplinks)
# downlink from broker
msg = types.SimpleNamespace(topic="msh/2/e/TA_Mesh/!abcdef01", payload=b"\x09\x09")
r._on_message(None, None, msg)
echo = types.SimpleNamespace(topic="msh/2/e/TA_Mesh/!12345678", payload=b"x")
r._on_message(None, None, echo)
print("downlink", calls["radio"], r.downlinks, "echoes dropped", r.dropped_echoes)
# proxy turned off on radio
off = make_iface(); off.localNode.moduleConfig.mqtt.proxy_to_client_enabled = False; r.attach(off); print("off ->", r.status)
r.stop(); print("stopped", r.status)
