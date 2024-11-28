import ckan.plugins as plugins
from . import iotrans, utils
from memory_profiler import profile, memory_usage
import json
from datetime import datetime

def write_results(resource_id, mem, val):
    when = datetime.now().strftime("%Y_%m_%dT%H_%M")
    file_prefix = f"{when}_{resource_id}_"
    with open(f"{file_prefix}mem.log", "w") as file:
        for reading in mem:
            file.write(f"{reading}\n")
    with open(f"{file_prefix}val.json", "w") as file:
        json.dump(val, file)


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
        @click.argument('resourceid')
        @click.argument('targetformat')
        def ioprofile(resourceid, targetformat):

            data = {
                "resource_id": resourceid,
                "source_epsg": 4326,
                "target_epsgs": [4326, 2952],
                "target_formats": ["shp"],
            }
            
            @profile
            def to_file_wrapper():
                iotrans.to_file({"ignore_auth": True}, data)

            mem, val = memory_usage((to_file_wrapper, (), {}), retval=True)
            write_results(resourceid, mem, val)
            print(mem)
            print(val)
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
