import json
from neo4j import GraphDatabase

from settings import N4J_URI, N4J_USER, N4J_PASSWORD
from measure.relation import RelationMixin
from measure import _collect_time


class Neo4j(RelationMixin):
    def __init__(self, report_dir, explain=False):
        self.n4j = GraphDatabase.driver(N4J_URI, auth=(N4J_USER, N4J_PASSWORD))
        self.session = self.n4j.session()
        self.report_dir = report_dir
        self.explain = explain
        self.times = []

    def _create_entry(self, parameter, resp=None, spent_time=None):
        result = '|'.join(sorted(resp))
        return {
            'uuids': f'{parameter[0]}_{parameter[1]}',
            'length': parameter[2],
            'result': result,
            'spent_time': spent_time,
        }

    @_collect_time(_create_entry)
    def query(self, parameter):
        def work(tx, uuid1, uuid2, length):
            cql = '''
                match (e0:Entry) -- (e1:Entry) -[*%s..%s]- (e2:Entry)
                where e1.uuid = $uuid1 and e2.uuid = $uuid2
                return e1.uuid
            ''' % (length, length)
            if self.explain:
                cql = f'explain {cql}'
            result = tx.run(cql, {
                'uuid1': uuid1,
                'uuid2': uuid2,
            })
            if self.explain:
                return result.consume().plan
            return result.value('e1.uuid')
        return self.session.read_transaction(work, *parameter)

    def _get_explain_glob(self):
        return f'{self.report_dir}/rel.n4j.*.txt'

    def _get_time_report_path(self):
        return f'{self.report_dir}/rel.n4j.csv'

    def _report_explain(self, parameter, content):
        with open(f'{self.report_dir}/rel.n4j.{parameter[0]}_{parameter[1]}_{parameter[2]}.txt', 'w') as f:
            f.write(json.dumps(content, indent=4))

    def close(self):
        self.n4j.close()
