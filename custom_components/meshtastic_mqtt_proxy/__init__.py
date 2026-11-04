# SPDX-License-Identifier: GPL-3.0-or-later
"""Meshtastic MQTT client proxy: relays MQTT both ways for a USB radio in Meshtastic UI."""

from __future__ import annotations

import logging
from datetime import timedelta
from importlib.metadata import PackageNotFoundError, version

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval

from .relay import MqttProxyRelay

_LOGGER = logging.getLogger(__name__)

DOMAIN = "meshtastic_mqtt_proxy"
SOURCE_DOMAIN = "meshtastic_ui"
PLATFORMS = [Platform.SENSOR]
POLL = timedelta(seconds=15)


def _current_interface(hass: HomeAssistant):
    """The interface Meshtastic UI currently holds (first connected radio)."""
    entries = hass.data.get(SOURCE_DOMAIN, {}).get("entries", {})
    for entry_data in entries.values():
        connection = entry_data.get("connection") if isinstance(entry_data, dict) else None
        iface = getattr(connection, "interface", None)
        if iface is not None and getattr(iface, "myInfo", None) is not None:
            return iface
    return None


def _library_version() -> str:
    try:
        return version("meshtastic")
    except PackageNotFoundError:
        return "not installed"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    lib = await hass.async_add_executor_job(_library_version)  # reads dist-info: blocking I/O
    _LOGGER.info("Meshtastic MQTT proxy starting (meshtastic library %s)", lib)

    relay = MqttProxyRelay()
    relay.start()

    @callback
    def _poll(_now=None) -> None:
        # Pick up the radio at startup and after Meshtastic UI reconnects
        # (each reconnect creates a new interface object).
        iface = _current_interface(hass)
        if iface is not None and iface is not relay.interface:
            hass.async_add_executor_job(relay.attach, iface)

    entry.async_on_unload(async_track_time_interval(hass, _poll, POLL))
    _poll()

    entry.runtime_data = relay
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if ok:
        await hass.async_add_executor_job(entry.runtime_data.stop)
    return ok
