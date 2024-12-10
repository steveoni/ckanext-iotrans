import ckan.plugins as plugins
from . import utils
from . import iotrans


class IotransPlugin(plugins.SingletonPlugin):
    """
    # ==============================
    # IActions
    # ==============================
    These are custom api endpoints
    ex: hitting <ckan_url>/api/action/to_file will trigger
    the api.to_file function

    These can also be used with tk.get_action("to_file"),
    for example, in this CKAN extension code
    """

    plugins.implements(plugins.IActions)

    def get_actions(self):
        return {
            "to_file": iotrans.to_file,
            "prune": iotrans.prune,
        }

    """
    # ==============================
    # IAuthFunctions
    # ==============================
    These are the auth rules for the above actions
    """
    plugins.implements(plugins.IAuthFunctions)

    def get_auth_functions(self):
        return {
            "to_file": utils.iotrans_auth_function,
            "prune": utils.iotrans_auth_function,
        }
