import logging
from elastic_utils.elastic_manager import ES
from elastic_utils.index_management import create_default_index_and_alias, reindex_alias
from elastic_utils.index_management import recreate_default_template
import argparse
import ConfigParser

logger = logging.getLogger(__name__)

config = ConfigParser.ConfigParser()
config.read('config.ini')

manager = ES()

parser = argparse.ArgumentParser("Read the local config.ini file")
parser.add_argument('--apply-template', help='Apply The template to the index define in the config',
                    action='store_true')
parser.add_argument('--reindex-alias', help="""Reindex the alias define in the config and map it
to a new physical index then delete the old physical index""",
                    action='store_true')
parser.add_argument('--create-alias', help='Create the alias define in the config and map it to a new physical index',
                    action='store_true')

args = vars(parser.parse_args())

has_arg = False
for key, value in args.items():
    if value:
        has_arg = True
        break

if not has_arg:
    print "You need at least one arg"
    parser.print_help()
    exit(0)

manager.add_connection(config.get("default", "connection"))

if args.get("apply_template"):
    template_name = config.get("default", "template_name")
    with open(config.get("default", "template_path")) as f:
        recreate_default_template(manager.get_connection(), template_name, f.read())


if args.get("reindex_alias"):
    alias_name = config.get("default", "default_alias")
    reindex_alias(manager.get_connection(), alias_name)


if args.get("create_alias"):
    alias_name = config.get("default", "default_alias")
    create_default_index_and_alias(manager.get_connection(), alias_name)



#create_default_index_and_alias(manager.get_connection(), "go.benoit.test")
#create_new_write_index(manager.get_connection(), "go.benoit.test")
#change_read_alias_to_write_alias(manager.get_connection(), "go.benoit.test")


#
#