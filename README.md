# Meshtastic MQTT proxy for Home Assistant

Relays MQTT **both ways** for a Meshtastic radio that the
[Meshtastic UI](https://github.com/Daring-Designs/meshtastic-ui-ha) integration holds over USB serial.

## Why

Radios without their own network (nRF52 boards such as the ProMicro DIY) run MQTT in
*client proxy* mode (`module_config.mqtt.proxy_to_client_enabled = true`): the radio hands its
MQTT traffic to whatever client is connected and expects that client to talk to the broker in
both directions. The Meshtastic phone apps do this. Home Assistant integrations did not:

| | uplink (radio → broker) | downlink (broker → radio) |
|---|---|---|
| Meshtastic UI v1.1.2 / v1.2.0-rc.1 | no | no |
| broglep/homeassistant-meshtastic 0.6.1 | yes | **no** (never subscribes) |
| meshtastic Python library 2.7.x | provides hooks only | provides hooks only |

So while Home Assistant held the radio, nothing from the MQTT side of the mesh ever arrived.
Packets heard over LoRa did arrive, but most nodes in an MQTT-bridged mesh are only reachable
through the broker. Diagnosed 2026-11-04.

## How it works

It is a separate custom integration and does not change Meshtastic UI's code. For the relay it uses
two hooks the meshtastic Python library provides for client proxy implementations (python PR #790, merged
June 2025):

- **Uplink:** the library publishes every `FromRadio.mqttClientProxyMessage` on the pypubsub topic
  `meshtastic.mqttclientproxymessage` (with `proxymessage`, `interface`). The relay publishes it to
  the broker unchanged (QoS 1, `retained` as given).
- **Downlink:** the relay subscribes to the same topics the firmware would subscribe to if it had
  its own network (`MQTT::sendSubscriptions()` in firmware `src/mqtt/MQTT.cpp`):
  `<root>/2/e/<channel>/+` for each channel with downlink enabled, plus `<root>/2/e/PKI/+`. The
  channel name falls back to the modem preset name (`LongFast`, …) like `Channels::getName()`. Each
  message goes to the radio with `interface.sendMqttClientProxyMessage(topic, payload)`.
- Echoes of our own uplinks (`…/!<own node id>`) are dropped. The firmware would drop them anyway
  ("Ignore downlink message we originally sent").

The broker address, port (`host:port` in `address`), username, password, TLS flag and root topic are
read from the **radio's own MQTT module config** at runtime. Nothing is stored in Home Assistant,
and the credentials are never logged.

The relay polls Meshtastic UI every 15 s for its current interface
(`hass.data["meshtastic_ui"]["entries"][…]["connection"].interface`), so it follows reconnects.

### Names on a mesh that mixes licensed and unlicensed nodes

Since July 2025 (firmware `c3b2b474c`, in v2.7.15 but not v2.6.11) a radio drops any NODEINFO whose
`is_licensed` flag differs from its own: `NodeInfoModule.cpp` returns "handled", so the name is neither
saved nor passed to the client. The check works both ways. A licensed radio loses the names of
unlicensed nodes, and an unlicensed radio loses those of licensed nodes. Their other packets still
arrive, so these nodes show up as `Meshtastic 23f0` with hardware `UNSET`, the placeholder the Python
library creates in `_getOrCreateByNum()`. Meshtastic UI stores that placeholder as a real name, so its
own request for missing names never fires either.

The packets are still on the broker, so the relay reads them there:

- It looks at each downlink, including echoes of our own uplinks, because a NODEINFO the radio heard
  over LoRa may come back that way. It saves every name, but applies one right away only if the
  licensed flag differs from the radio's; the rest are left to the radio.
- It writes the name into the library's node table, as the library's own `_onNodeInfoReceive()` does,
  and publishes `meshtastic.node.updated`, so Meshtastic UI updates by its normal path. This also fixes
  the sender for nodes that showed as `unknown`.
- Names are saved in `.storage/meshtastic_mqtt_proxy.names`. A new interface starts with placeholders
  for every node the radio has no user record for, which also happens now and then to nodes with the
  same licensed flag. After a restart, saved names replace placeholders only, and are written directly
  to Meshtastic UI's node store (`bulk_update_nodes`), because the event would mark every node as heard
  just now.

Limits: only plaintext NODEINFO can be read, which is what a licensed radio's channel carries. A node's
name appears only after it announces itself once with the relay running, and the firmware default is
every 3 hours. Names come from unauthenticated broadcasts, as they do on the radio. The node id is
always derived from the sender, as in the firmware. The `names_learned` attribute on the sensor counts
the saved names.

## Install

1. Copy `custom_components/meshtastic_mqtt_proxy` to `/config/custom_components/` and restart.
2. Settings → Devices & services → Add integration → *Meshtastic MQTT proxy* (no options).
3. The radio needs MQTT enabled with *proxy to client* on, and Meshtastic UI must be loaded and connected.

The sensor `sensor.meshtastic_mqtt_proxy` reports the state (`waiting_for_radio`, `connecting`,
`connected`, `reconnecting`, `proxy_off_on_radio`, `error`, `stopped`), with the attributes
`broker`, `subscriptions`, `uplinks`, `downlinks`, `dropped_echoes` and `last_error`.

## Versions and failure points

- Tested with Home Assistant 2026.10.4 (Python 3.14), meshtastic 2.7.11, paho-mqtt 2.1.0,
  Meshtastic UI 1.1.2, and a NRF52_PROMICRO_DIY radio on the TA_Mesh broker.
- Needs `MeshInterface.sendMqttClientProxyMessage` (meshtastic ≥ 2.6.x). If it is missing, the sensor
  goes to `error` with a message saying so.
- It relies on Meshtastic UI exposing `connection.interface`. If a Meshtastic UI update renames that,
  the sensor stays at `waiting_for_radio` until the first uplink frame arrives. Uplink frames carry
  the interface too, so the relay still attaches then.
- Names use the library's private `_getOrCreateByNum()`, `nodes` and Meshtastic UI's `store` entry.
  If those change, the relay keeps working and names stop being learned (logged at debug level).
- One radio only: the first connected Meshtastic UI entry wins.
- If the phone app connects to the same radio over Bluetooth, it may also proxy. The firmware
  de-duplicates packets by ID.
- The MQTT client id is `!<node id>-ha`, so it doesn't kick a phone app that uses the bare node id.

## Test

`tests/test_relay.py` exercises topic derivation, uplink, downlink, echo dropping and the
"proxy off" case and name learning, with real meshtastic protobufs and a fake paho client. It makes no network
connections:

```
python -m venv venv && venv/bin/pip install meshtastic paho-mqtt
venv/bin/python -I tests/test_relay.py custom_components/meshtastic_mqtt_proxy
```

## Verified on a live mesh (2026-11-04)

This setup is an nRF52840 radio on USB, with Home Assistant as the only client, on the TA_Mesh
community mesh (broker `mqtt.tamesh.org`).

- **Before the relay:** Meshtastic UI never stored a single received text message. Its received-packet
  counter stayed at 0 over 360 samples. The debug log showed the radio emitting
  `mqttClientProxyMessage` frames that nothing handled.
- **With the relay:** within 2.5 minutes there were 7 downlinks and 1 uplink, and nodes reachable only
  over MQTT started appearing in Meshtastic UI (80 → 83 known nodes, 3 → 6 active).
- **Round trip:** the command `/wx 34` was sent from Meshtastic UI on the primary channel. Two seconds
  later, a weather bot reachable only over MQTT (`via_mqtt: true`) replied with the Istanbul weather,
  and the reply arrived in Meshtastic UI. A plain `test` broadcast went out and came back from the
  broker as an echo, which was dropped.

## Things that look wrong but aren't the relay

These are Meshtastic UI 1.1.2 behaviours, noted here for whoever looks next:

- **Outgoing broadcasts show `failed (MAX_RETRANSMIT)`.** The radio marks a broadcast delivered only
  when it hears another radio repeat it over LoRa (the implicit ACK). With no LoRa neighbour repeating
  it, the status says failed even though the message went out over MQTT. The reply above came back
  despite that status.
- **Senders show as `unknown`, and a reply can be stored twice, once from `unknown`.** Each packet
  names its sender only by number (`from`). The meshtastic library's `_nodeNumToId()` returns `None`
  until it has a record for that node, and Meshtastic UI's `normalize_node_id()` turns `None` into
  `"unknown"` instead of falling back to `packet["from"]`. For the same reason, its received-packet
  counter skips these packets, and messages stored before the node became known keep `unknown`.
  Behind an MQTT client proxy this mostly hits nodes right after setup. See
  [Names](#names-on-a-mesh-that-mixes-licensed-and-unlicensed-nodes) for nodes whose name never arrives.
- **Stats report `channel_count: 0`** even when the radio has a primary channel.

## License

GPL-3.0-or-later. See `LICENSE`.
