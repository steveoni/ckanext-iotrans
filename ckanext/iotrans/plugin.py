import ckan.plugins as plugins
from . import iotrans, utils


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
    plugins.implements(plugins.IClick)


    def get_commands(self):
        import click
        @click.command()
        def ioprofile():
            v= iotrans.to_file({"ignore_auth": True}, {
                "resource_id": "695cbc0a-480c-493c-8279-3c0537e4e950",
                "source_epsg": 4326,
                 "target_epsgs": [4326, 2952],
                 "target_formats": ["shp","gpkg", "geojson"]})
            print(v)
        return [ioprofile]

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
