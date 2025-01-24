import ckan.plugins as plugins

from . import iotrans
from .utils import generic


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
        def tofile(resourceid, targetformat):

            data = {
                "resource_id": resourceid,
                "source_epsg": 4326,
                "target_epsgs": [2952],
                "target_formats": targetformat.strip().split(","),
            }
           
            try:
                result = iotrans.to_file({"ignore_auth": True}, data)
                click.echo(click.style(str(result), fg="green"))
            except Exception as e:
                click.echo(click.style("❌ An error occurred during the operation:", fg="red", bold=True))
                click.echo(click.style(str(e), fg="red"))
                return

        return [tofile]

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
            "to_file": generic.iotrans_auth_function,
            "prune": generic.iotrans_auth_function,
        }
