from utils import Timer, new_index_from_name
from elasticsearch import helpers
import logging

logger = logging.getLogger(__name__)


def get_index_from_alias(es_client, alias_name):

    if es_client.indices.exists_alias(alias_name):
        indexes = [keys for keys in es_client.indices.get_alias(alias_name).keys()]
        return indexes
    else:
        return None


def recreate_default_template(es_client, template_name, json_string):
    es_client.indices.put_template(template_name, body=json_string)


def create_default_index_and_alias(es_client, alias_name, index_body=None):
    default_alias_name = alias_name
    logger.info("Default alias name: {}".format(default_alias_name))
    physical_index = get_index_from_alias(es_client, default_alias_name)
    if not physical_index:
        with Timer('Creating Index and Alias', logger=logger.info):
            index_name = new_index_from_name(default_alias_name)
            es_client.indices.create(index_name, body=index_body)
            es_client.indices.put_alias(default_alias_name, index_name)
            es_client.indices.put_alias(default_alias_name + ".write", index_name)
    else:
        logger.info("Alias {} exist mapped to {}".format(alias_name, physical_index))


def flush_index(es_client, index_name):
    es_client.indices.flush(index_name)


def reindex_percolators(source_client, source_index, target_index, dest_client=None):
    dest_client = source_client if dest_client is None else dest_client

    docs = helpers.scan(source_client, index=source_index, doc_type=".percolator")
    def _change_doc_index(hits, index):
        for h in hits:
            h['_index'] = index
            yield h

    return helpers.bulk(dest_client, _change_doc_index(docs, target_index),
                        chunk_size=500, stats_only=True)

def reindex(source_client, source_index, target_index, dest_client=None):
    #return success, error_list
    success, error_list = reindex_percolators(source_client, source_index, target_index, dest_client)
    logger.info("# Percolators {} success".format(success))
    if error_list:
        logger.error("# Percolators {} errors".format(len(error_list)))
        for error in error_list[:10]:
            logger.error("Error Percolators: {}".format(error))
    success, error_list =  helpers.reindex(source_client, source_index, target_index, dest_client)
    logger.info("# Documents {} success".format(success))
    if error_list:
        logger.error("# Documents {} errors".format(len(error_list)))
        for error in error_list[:10]:
            logger.error("Error Documents: {}".format(error))



def create_new_write_index(es_client, alias_name, new_index_config=None):

    with Timer('Create Write Index and change Alias', logger=logger.info):
        write_alias = alias_name + ".write"

        #Get the current Real Index for the write alias
        current_indexes = get_index_from_alias(es_client, write_alias)
        current_index = current_indexes[0]

        #Create a new empty index for writes
        new_index_name = new_index_from_name(alias_name)
        es_client.indices.create(new_index_name, new_index_config)
        es_client.indices.flush()
        #Change the write alias to the new index for writes
        es_client.indices.update_aliases({"actions": [
            {"remove": {"index": current_index, "alias": write_alias}},
            {"add": {"index": new_index_name, "alias": write_alias}}
        ]})
        es_client.indices.flush()


def change_read_alias_to_write_alias(es_client, alias_name):
    with Timer('Change Read Alias To Write Alias', logger=logger.info):
        #Get the current Real Index for the write alias
        current_write_indexes = get_index_from_alias(es_client, alias_name + ".write")
        current_write_index = current_write_indexes[0]

        current_read_indexes = get_index_from_alias(es_client, alias_name)
        current_read_index = current_read_indexes[0]

        es_client.indices.update_aliases({"actions": [
            {"remove": {"index": current_read_index, "alias": alias_name}},
            {"add": {"index": current_write_index, "alias": alias_name}}
        ]})
        es_client.indices.flush()

    return current_read_index


def reindex_alias(es_client, alias_name, new_index_config=None):
    create_new_write_index(es_client, alias_name, new_index_config)

    #same Alias but different for read and write
    with Timer('Reindex {}'.format(alias_name), logger=logger.info):
        reindex(es_client, alias_name, alias_name + ".write")

    #Move the read alias to new alias
    old_read_index = change_read_alias_to_write_alias(es_client, alias_name)

    #Move Delete old index
    with Timer('Delete old Read index', logger=logger.info):
        es_client.indices.delete(old_read_index)