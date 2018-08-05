# coding: utf-8
# zhangxiaoyang.hit#gmail.com
# github.com/zhangxiaoyang

import configparser
import json
import socket
import traceback

config = configparser.ConfigParser()
config.read('config.ini')


class GstoreConnector:
    _instance = None

    def _connect(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.ip, self.port))
            return True
        except Exception, e:
            print 'socket connection error. @GstoreConnector.connect'
            traceback.print_exc()
        return False

    def _disconnect(self):
        try:
            self._sock.close()
            return True
        except Exception, e:
            print 'socket disconnection error. @GstoreConnector.disconnect'
            traceback.print_exc()
        return False

    def _send(self, msg):
        data = self._pack(msg)
        self._sock.send(data)
        return True

    def _recv(self):
        head = self._sock.recv(4)
        context_len = 0
        for i in range(4):
            context_len |= (ord(head[i]) & 0xFF) << i * 8

        data = bytearray()
        recv_len = 0
        while recv_len < context_len:
            chunk = self._sock.recv(context_len - recv_len)
            data.extend(chunk)
            recv_len += len(chunk)
        return data.rstrip('\x00').decode('utf-8')

    def _pack(self, msg):
        data_context = bytearray(msg, encoding='utf-8')
        context_len = len(data_context) + 1
        data_len = context_len + 4

        data = bytearray(data_len)
        for i in range(4):
            data[i] = chr((context_len >> i * 8) & 0xFF)
        for i, _ in enumerate(data_context):
            data[i + 4] = data_context[i]
        data[data_len - 1] = 0
        return data

    def _communicate(f):
        def wrapper(self, *args, **kwargs):
            if not self._connect():
                print 'connect to server error. @GstoreConnector.%s' % f.__name__
                return False

            if f.__name__ == 'build':
                cmd = 'import'
            elif f.__name__ == 'show':
                if args[0]:
                    cmd = 'show all'
                else:
                    cmd = 'show databases'
            else:
                cmd = f.__name__
            if f.__name__ == 'query':
                # DEBUG:only sparql query should be used, output should not be included in parameters
                print "the first argument:"
                print args[0]
                # print args[1]
                params = ' '.join([args[0]])
            else:
                params = ' '.join(map(lambda x: str(x), args))
            full_cmd = ' '.join([
                cmd,
                params
            ]).strip()
            print "command to send to server:"
            print full_cmd

            if not self._send(full_cmd):
                print 'send %s command error. @GstoreConnector.build' % cmd
                return False

            recv_msg = self._recv()
            self._disconnect()

            succ = {
                'test': 'OK',
                'load': 'load database done.',
                'unload': 'unload database done.',
                'import': 'import RDF file to database done.',
                'drop': 'drop database done.',
                'stop': 'server stopped.',
                'query': None,
                'show all': None,
                'show databases': None,
            }
            if cmd in succ:
                if succ[cmd] == recv_msg:
                    return True
                elif 'query' == cmd:
                    if recv_msg == u'query failed.':
                        return recv_msg
                    answer_obj = json.loads(recv_msg)['results']['bindings']
                    for i in range(len(answer_obj)):
                        answer_obj[i] = {k: answer_obj[i][k]['value'] for k in answer_obj[i]}
                        for k in answer_obj[i]:
                            if answer_obj[i][k].startswith('http://'):
                                answer_obj[i][k] = answer_obj[i][k].replace('\'', '%27').replace('\"', '%22').replace(' ', '%20')
                    return answer_obj
                else:
                    return recv_msg
            return False

        return wrapper

    def __init__(self, ip='127.0.0.1', port=3305):
        self.ip = ip
        self.port = port

    @_communicate
    def test(self):
        pass

    @_communicate
    def load(self, db_name):
        pass

    @_communicate
    def unload(self, db_name):
        pass

    @_communicate
    def build(self, db_name, rdf_file_path):
        pass

    @_communicate
    def drop(self, db_name):
        pass

    @_communicate
    def query(self, sparql, output='/'):
        pass

    @_communicate
    def show(self, _type=False):
        pass

    @staticmethod
    def get_instance():
        if GstoreConnector._instance is None:
            GstoreConnector._instance = GstoreConnector(config.get('DB', 'host'), int(config.get('DB', 'port')))
            GstoreConnector._instance.unload()
            GstoreConnector._instance.load(config.get('DB', 'dataset'))
        return GstoreConnector._instance

    @staticmethod
    def get_genres():
        sparql = '''select distinct ?genre 
        where {
            ?x <http://dbpedia.org/ontology/genre> ?genre. 
            ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://dbpedia.org/ontology/MusicalWork>.
            }'''
        answer_obj = GstoreConnector.get_instance().query(sparql)
        assert isinstance(answer_obj, list)
        return [{'name': ele['genre'][len('<http://dbpedia.org/resource'):-1].replace('_', ' '),
                 'uri': ele['genre']} for ele in answer_obj]

    @staticmethod
    def get_subjects(name, res_type, genre):
        where_sql = ''
        if not name and not res_type and not genre:
            return []
        if not res_type:
            return GstoreConnector.get_subjects(name, 'Album', genre) \
                   + GstoreConnector.get_subjects(name, 'Single', genre)

        name = name.replace('"', '\"').replace('_', ' ')
        if genre:
            where_sql += '?uri <http://dbpedia.org/ontology/genre> <{}>.\n'.format(genre)
        if res_type:
            where_sql += '?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/{}>.\n'.format(
                res_type)
        if name:
            where_sql += '?uri <http://xmlns.com/foaf/0.1/name> "{}"@en.\n'.format(name)
        sparql = '''
            select distinct ?uri
            where {%s}
            LIMIT 200
        ''' % where_sql
        answer_obj = GstoreConnector.get_instance().query(sparql)
        assert isinstance(answer_obj, list)
        for ele in answer_obj:
            ele['res_type'] = res_type
            ele['name'] = ele['uri'][ele['uri'].rindex('/') + 1:].replace('%27', '\'').replace('%22', '\"').replace('%20', ' ')
        return answer_obj

    @staticmethod
    def get_predicates_objects(subject_uri):
        sparql = '''
            select distinct ?pred ?objc
            where {
                %s ?pred ?objc.
            }
        ''' % subject_uri
        answer_obj = GstoreConnector.get_instance().query(sparql)
        assert isinstance(answer_obj, list)
        answer_dict = {ele['pred']: [] for ele in answer_obj}
        for ele in answer_obj:
            answer_dict[ele['pred']].append(ele['objc'])
        return [(k, answer_dict[k]) for k in answer_dict]