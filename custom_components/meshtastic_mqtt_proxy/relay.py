# SPDX-License-Identifier: GPL-3.0-or-later
"""MQTT client proxy for a Meshtastic radio held by the Meshtastic UI integration.

Radios without their own network (nRF52 boards) run MQTT in "client proxy"
mode: the radio hands MQTT traffic to the connected client, and the client is
expected to talk to the broker in both directions. The phone apps do this.
This relay does the same for a radio connected to Home Assistant over USB:

  uplink:   radio -> FromRadio.mqttClientProxyMessage -> pubsub topic
            "meshtastic.mqttclientproxymessage" -> publish to the broker
  downlink: broker -> subscribed topics -> interface.sendMqttClientProxyMessage()
            -> ToRadio.mqttClientProxyMessage -> radio

Both hooks are part of the meshtastic Python library (>= 2.6.x, PR #790).
Subscriptions mirror the firmware's own MQTT::sendSubscriptions(): one
"<root>/2/e/<channel>/+" per downlink-enabled channel plus "<root>/2/e/PKI/+".
Broker address and credentials are read from the radio's MQTT module config
at runtime; they are never stored or logged here.
"""

from __future__ import annotations

import logging
import ssl
import threading
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)

PROXY_TOPIC = "meshtastic.mqttclientproxymessage"
MAX_QUEUED_UPLINKS = 200

# DisplayFormatters::getModemPresetDisplayName() in the firmware.
_PRESET_NAMES = {
    "LONG_FAST": "LongFast", "LONG_SLOW": "LongSlow", "LONG_MODERATE": "LongMod",
    "VERY_LONG_SLOW": "VLongSlow", "MEDIUM_FAST": "MediumFast", "MEDIUM_SLOW": "MediumSlow",
    "SHORT_FAST": "ShortFast", "SHORT_SLOW": "ShortSlow", "SHORT_TURBO": "ShortTurbo",
    "LONG_TURBO": "LongTurbo",
}


@dataclass(frozen=True)
class BrokerSettings:
    """What the radio's MQTT module config asks for."""

    host: str
    port: int
    username: str = field(repr=False)
    password: str = field(repr=False)
    tls: bool
    topics: tuple[str, ...]
    gateway_id: str


def _channel_name(node: Any, channel: Any) -> str:
    """Channels::getName(): the channel name, or the modem preset name if empty."""
    if channel.settings.name:
        return channel.settings.name
    lora = node.localConfig.lora
    if not lora.use_preset:
        return "Custom"
    preset = lora.DESCRIPTOR.fields_by_name["modem_preset"].enum_type.values_by_number[lora.modem_preset].name
    return _PRESET_NAMES.get(preset, preset.title().replace("_", ""))


def read_settings(iface: Any) -> BrokerSettings | None:
    """Return broker settings if the radio wants a client proxy, else None."""
    node = iface.localNode
    mqtt_cfg = node.moduleConfig.mqtt
    if not (mqtt_cfg.enabled and mqtt_cfg.proxy_to_client_enabled):
        return None
    tls = bool(mqtt_cfg.tls_enabled)
    address = mqtt_cfg.address or "mqtt.meshtastic.org"
    host, _, port = address.partition(":")
    root = mqtt_cfg.root or "msh"
    crypt = f"{root}/2/e/"
    topics = [f"{crypt}{_channel_name(node, ch)}/+" for ch in node.channels if ch.settings.downlink_enabled]
    if topics:
        topics.append(f"{crypt}PKI/+")
    return BrokerSettings(
        host=host,
        port=int(port) if port else (8883 if tls else 1883),
        username=mqtt_cfg.username,
        password=mqtt_cfg.password,
        tls=tls,
        topics=tuple(topics),
        gateway_id=f"!{iface.myInfo.my_node_num:08x}",
    )


class MqttProxyRelay:
    """Relays MQTT traffic between one Meshtastic interface and its broker."""

    def __init__(self) -> None:
        self._lock = threading.RLock()        # config / client lifecycle
        self._send_lock = threading.Lock()    # serialises downlink writes to the radio
        self._iface: Any = None
        self._settings: BrokerSettings | None = None
        self._client: mqtt.Client | None = None
        self.status = "waiting_for_radio"
        self.uplinks = 0
        self.downlinks = 0
        self.dropped_echoes = 0
        self.last_error: str | None = None

    # ---- lifecycle -------------------------------------------------------

    def start(self) -> None:
        from pubsub import pub

        pub.subscribe(self._on_proxy_message, PROXY_TOPIC)

    def stop(self) -> None:
        from pubsub import pub

        try:
            pub.unsubscribe(self._on_proxy_message, PROXY_TOPIC)
        except Exception:  # noqa: BLE001
            pass
        with self._lock:
            self._stop_client()
            self._iface = None
            self.status = "stopped"

    def attach(self, iface: Any) -> None:
        """Use this interface; (re)connect to the broker if its settings changed."""
        if not hasattr(iface, "sendMqttClientProxyMessage"):
            self._fail("meshtastic library too old: no sendMqttClientProxyMessage (needs >= 2.6.x)")
            return
        try:
            settings = read_settings(iface)
        except Exception as err:  # noqa: BLE001  config not downloaded yet
            _LOGGER.debug("Radio config not readable yet: %s", err)
            return
        with self._lock:
            self._iface = iface
            if settings is None:
                self._stop_client()
                self._settings = None
                self.status = "proxy_off_on_radio"
                return
            if settings == self._settings and self._client is not None:
                return
            self._stop_client()
            self._settings = settings
            self._start_client(settings)

    @property
    def interface(self) -> Any:
        return self._iface

    @property
    def broker(self) -> str | None:
        s = self._settings
        return f"{s.host}:{s.port}" if s else None

    @property
    def topics(self) -> list[str]:
        return list(self._settings.topics) if self._settings else []

    # ---- MQTT client -----------------------------------------------------

    def _start_client(self, s: BrokerSettings) -> None:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{s.gateway_id}-ha",
                             protocol=mqtt.MQTTv311)
        if s.username:
            client.username_pw_set(s.username, s.password or None)
        if s.tls:
            client.tls_set_context(ssl.create_default_context())
        client.max_queued_messages_set(MAX_QUEUED_UPLINKS)
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        self.status = "connecting"
        _LOGGER.info("Meshtastic MQTT proxy connecting to %s:%s for %s", s.host, s.port, s.gateway_id)
        client.connect_async(s.host, s.port, keepalive=60)
        client.loop_start()
        self._client = client

    def _stop_client(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:  # noqa: BLE001
                pass

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self._fail(f"broker refused connection: {reason_code}")
            return
        s = self._settings
        if s and s.topics:
            client.subscribe([(t, 1) for t in s.topics])
        self.status = "connected"
        self.last_error = None
        _LOGGER.info("Meshtastic MQTT proxy connected, subscribed to %s", ", ".join(s.topics) if s else "-")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        if self._client is client:
            self.status = "reconnecting"
            _LOGGER.warning("Meshtastic MQTT proxy lost the broker (%s), reconnecting", reason_code)

    # ---- relaying --------------------------------------------------------

    def _on_message(self, client, userdata, msg) -> None:
        """Downlink: broker -> radio."""
        s, iface = self._settings, self._iface
        if s is None or iface is None:
            return
        if msg.topic.endswith("/" + s.gateway_id):
            # Our own uplink echoed back; the firmware would drop it anyway.
            self.dropped_echoes += 1
            return
        try:
            with self._send_lock:
                iface.sendMqttClientProxyMessage(msg.topic, bytes(msg.payload))
            self.downlinks += 1
        except Exception as err:  # noqa: BLE001
            self._fail(f"downlink to radio failed: {err}")

    def _on_proxy_message(self, proxymessage, interface) -> None:
        """Uplink: radio -> broker. Runs in the meshtastic publishing thread."""
        if interface is not self._iface:
            self.attach(interface)
        client = self._client
        if client is None:
            return
        if proxymessage.HasField("data"):
            payload = proxymessage.data
        elif proxymessage.HasField("text"):
            payload = proxymessage.text.encode("utf-8")
        else:
            return
        info = client.publish(proxymessage.topic, payload, qos=1, retain=proxymessage.retained)
        if info.rc == mqtt.MQTT_ERR_SUCCESS or info.rc == mqtt.MQTT_ERR_NO_CONN:
            self.uplinks += 1  # NO_CONN: queued by paho until the broker is back
        else:
            self._fail(f"uplink publish failed: {mqtt.error_string(info.rc)}")

    def _fail(self, message: str) -> None:
        self.last_error = message
        self.status = "error"
        _LOGGER.warning("Meshtastic MQTT proxy: %s", message)
