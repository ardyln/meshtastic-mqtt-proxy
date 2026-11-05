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
from homeassistant.helpers.storage import Store

from .relay import MqttProxyRelay

_LOGGER = logging.getLogger(__name__)

DOMAIN = "meshtastic_mqtt_proxy"
SOURCE_DOMAIN = "meshtastic_ui"
PLATFORMS = [Platform.SENSOR]
POLL = timedelta(seconds=15)
NAMES_STORE_VERSION = 1
NAMES_SAVE_DELAY = 30  # seconds


def _current_radio(hass: HomeAssistant):
    """The interface Meshtastic UI currently holds (first connected radio) and its node store."""
    entries = hass.data.get(SOURCE_DOMAIN, {}).get("entries", {})
    for entry_data in entries.values():
        connection = entry_data.get("connection") if isinstance(entry_data, dict) else None
        iface = getattr(connection, "interface", None)
        if iface is not None and getattr(iface, "myInfo", None) is not None:
            return iface, entry_data.get("store")
    return None, None


def _library_version() -> str:
    try:
        return version("meshtastic")
    except PackageNotFoundError:
        return "not installed"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    lib = await hass.async_add_executor_job(_library_version)  # reads dist-info: blocking I/O
    _LOGGER.info("Meshtastic MQTT proxy starting (meshtastic library %s)", lib)

    names_store: Store[dict] = Store(hass, NAMES_STORE_VERSION, f"{DOMAIN}.names")

    def _save_names() -> None:
        hass.loop.call_soon_threadsafe(names_store.async_delay_save, relay.names_snapshot, NAMES_SAVE_DELAY)

    relay = MqttProxyRelay(await names_store.async_load(), _save_names)
    relay.start()

    async def _attach(iface, ui_store) -> None:
        await hass.async_add_executor_job(relay.attach, iface)
        names = relay.restore_names(iface)
        if names and ui_store is not None:
            # Not the node.updated event: that would mark every node as heard just now.
            ui_store.bulk_update_nodes(names)

    @callback
    def _poll(_now=None) -> None:
        # Pick up the radio at startup and after Meshtastic UI reconnects
        # (each reconnect creates a new interface object).
        iface, ui_store = _current_radio(hass)
        if iface is not None and iface is not relay.interface:
            entry.async_create_background_task(hass, _attach(iface, ui_store), "meshtastic_mqtt_proxy attach")

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
