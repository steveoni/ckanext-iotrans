import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from . import iotrans


class IotransPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IActions)

    # ==============================
    # IActions
    # ==============================
    # These are custom api endpoints
    # ex: hitting <ckan_url>/api/action/extract_info will trigger the api.extract_info function
    # These can also be used with tk.get_action("extract_info"), for example, in this CKAN extension code

    def get_actions(self):
        return {
            "to_file": iotrans.to_file,
            "prune": iotrans.prune,
        }

