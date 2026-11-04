# SPDX-License-Identifier: GPL-3.0-or-later
"""Config flow: a single instance, nothing to configure (settings come from the radio)."""

from __future__ import annotations

from homeassistant.config_entries import ConfigFlow

DOMAIN = "meshtastic_mqtt_proxy"


class MeshtasticMqttProxyFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        await self.async_set_unique_id(DOMAIN)
        self._abort_if_unique_id_configured()
        if user_input is None:
            return self.async_show_form(step_id="user")
        return self.async_create_entry(title="Meshtastic MQTT proxy", data={})
