# SPDX-License-Identifier: GPL-3.0-or-later
"""Status sensor for the Meshtastic MQTT client proxy."""

from __future__ import annotations

from datetime import timedelta

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

SCAN_INTERVAL = timedelta(seconds=30)
STATES = ["waiting_for_radio", "connecting", "connected", "reconnecting", "proxy_off_on_radio", "error", "stopped"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    async_add_entities([ProxyStatusSensor(entry)])


class ProxyStatusSensor(SensorEntity):
    """Relay state, with traffic counters as attributes (read from the relay, no I/O)."""

    _attr_has_entity_name = True
    _attr_name = "Meshtastic MQTT proxy"
    _attr_icon = "mdi:transit-connection-variant"
    _attr_device_class = SensorDeviceClass.ENUM
    _attr_options = STATES

    def __init__(self, entry: ConfigEntry) -> None:
        self._relay = entry.runtime_data
        self._attr_unique_id = f"{entry.entry_id}_status"

    @property
    def native_value(self) -> str:
        return self._relay.status

    @property
    def extra_state_attributes(self) -> dict:
        r = self._relay
        return {
            "broker": r.broker,
            "subscriptions": r.topics,
            "uplinks": r.uplinks,
            "downlinks": r.downlinks,
            "dropped_echoes": r.dropped_echoes,
            "names_learned": r.names_learned,
            "last_error": r.last_error,
        }
