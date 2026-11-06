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

# names: the radio drops NODEINFO whose licensed flag differs from its own
from meshtastic.protobuf import mqtt_pb2, portnums_pb2

def nodeinfo(sender, name, licensed, hw=mesh_pb2.HardwareModel.TBEAM):
    user = mesh_pb2.User(id=f"!{sender:08x}", long_name=name, short_name=name[:4], hw_model=hw, is_licensed=licensed)
    p = mesh_pb2.MeshPacket(to=0xffffffff, decoded=mesh_pb2.Data(portnum=portnums_pb2.NODEINFO_APP, payload=user.SerializeToString()))
    setattr(p, "from", sender)
    return mqtt_pb2.ServiceEnvelope(packet=p, channel_id="TA_Mesh", gateway_id="!abcdef01").SerializeToString()

def add_node_table(iface, licensed=True):
    def get_or_create(num):
        i = f"!{num:08x}"
        return iface.nodesByNum.setdefault(num, {"num": num, "user": {"id": i, "longName": f"Meshtastic {i[-4:]}", "shortName": i[-4:], "hwModel": "UNSET"}})
    me = {"num": 0x12345678, "user": {"id": "!12345678", "longName": "ME", "isLicensed": licensed}}
    iface.nodesByNum, iface.nodes = {0x12345678: me}, {"!12345678": me}
    iface._getOrCreateByNum = get_or_create
    iface.getMyUser = lambda: me["user"]

updated, saves = [], []
def on_updated(node, interface): updated.append(node["user"]["longName"])
pub.subscribe(on_updated, R.NODE_UPDATED_TOPIC)
r2 = R.MqttProxyRelay(on_names_changed=lambda: saves.append(1)); iface2 = make_iface(); add_node_table(iface2); r2.attach(iface2)
def rx(relay, payload, topic="msh/2/e/TA_Mesh/!abcdef01"):
    relay._on_message(None, None, types.SimpleNamespace(topic=topic, payload=payload))

rx(r2, nodeinfo(0xaaaa0001, "UNLICENSED", False))
assert iface2.nodes["!aaaa0001"]["user"]["longName"] == "UNLICENSED" and updated == ["UNLICENSED"] and len(saves) == 1
rx(r2, nodeinfo(0xaaaa0001, "UNLICENSED", False))
assert len(saves) == 1, "same name again is no change to save"
rx(r2, nodeinfo(0xaaaa0002, "LICENSED", True))
assert "!aaaa0002" not in iface2.nodes and r2.names_learned == 2, "saved, but left to the radio for now"
rx(r2, nodeinfo(0xaaaa0003, "HEARD ON LORA", False), topic="msh/2/e/TA_Mesh/!12345678")
assert iface2.nodes["!aaaa0003"]["user"]["longName"] == "HEARD ON LORA", "read from our own echo"
rx(r2, nodeinfo(0x12345678, "SPOOF OF ME", False))
assert iface2.nodes["!12345678"]["user"]["longName"] == "ME"
before = r2.downlinks
rx(r2, b"\xff\xff not protobuf"); rx(r2, nodeinfo(0xaaaa0004, "", False))
assert r2.downlinks == before + 2 and r2.names_learned == 3, "bad packets are still relayed, and teach nothing"
rx(r2, nodeinfo(0xaaaa0001, "NOW LICENSED", True))
assert r2.names_snapshot()["!aaaa0001"]["longName"] == "NOW LICENSED", "a later restore uses the newest name"

# names survive a new interface (HA restart / reconnect); a real name from the radio wins
r3 = R.MqttProxyRelay({"!aaaa0003": {"id": "!aaaa0003", "longName": "SAVED", "shortName": "SAVE", "hwModel": "TBEAM"},
                       "!aaaa0005": {"id": "!aaaa0005", "longName": "OLD", "shortName": "OLD", "hwModel": "TBEAM"}})
iface3 = make_iface(); add_node_table(iface3); iface3._getOrCreateByNum(0xaaaa0005)["user"]["longName"] = "FROM RADIO"
assert r3.restore_names(iface3) == {}, "interface not attached yet"
r3.attach(iface3)
restored = r3.restore_names(iface3)
assert restored == {"!aaaa0003": {"name": "SAVED", "short_name": "SAVE", "hardware_model": "TBEAM"}}, restored
assert iface3.nodes["!aaaa0003"]["user"]["longName"] == "SAVED"
assert iface3.nodesByNum[0xaaaa0005]["user"]["longName"] == "FROM RADIO"
assert r3.restore_names(iface3) == {}, "once per interface"
r2.stop(); r3.stop(); print("names ok")

# time: sent to the radio's own node; a failure neither raises nor counts as synced
sent, r4 = [], R.MqttProxyRelay()
r4.sync_time(); assert r4.last_time_sync is None, "no radio yet"
iface4 = make_iface(); iface4.localNode.setTime = lambda: sent.append(1); r4.attach(iface4)
r4.sync_time(); assert sent == [1] and r4.last_time_sync
def broken(): raise OSError("serial gone")
iface5 = make_iface(); iface5.localNode.setTime = broken; r5 = R.MqttProxyRelay(); r5.attach(iface5)
r5.sync_time(); assert r5.last_time_sync is None
r4.stop(); r5.stop(); print("time ok")

# proxy turned off on radio
off = make_iface(); off.localNode.moduleConfig.mqtt.proxy_to_client_enabled = False; r.attach(off); print("off ->", r.status)
r.stop(); print("stopped", r.status)
