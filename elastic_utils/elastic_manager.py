from utils import singleton
import elasticsearch
import logging

logger = logging.getLogger(__name__)


@singleton
class ES(object):
    def __init__(self):
        self.connections = {}

    def add_connection(self, connection_string, name='default'):
        self.connections[name] = ElasticClient(connection_string)

    def get_connection(self, name='default'):
        """
        :param name: str
        :return: elasticsearch.Elasticsearch
        """
        return self.connections[name]


class ElasticClient(elasticsearch.Elasticsearch):
    def __init__(self, *args, **kwargs):
        super(ElasticClient, self).__init__(*args, **kwargs)
