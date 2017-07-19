from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from elasticsearch_dsl import Search, Q

from zenpy import Zenpy

host = '10.10.7.224'
es = Elasticsearch(host=host, timeout=120)
index = 'zendesk_full.sync'
creds = {
    'email': 'martin@magemojo.com/token',
    'password': 'wyrz93ljMxGl0w35UkCEm3hYTIQu7ZqP9hiysRsY',
    'subdomain': 'magemojo'
}

zendesk = Zenpy(**creds)


def form_action_event(events):
    res = []
    for event in events:
        if type(event) != dict:
            event = event.to_dict()
        for child in event['child_events']:
            if child['event_type'] == 'Comment':
                child['ticket_id'] = event['ticket_id']
                child['@timestamp'] = child['created_at']
                res.append({'_op_type': 'index',
                            '_index': index,
                            '_type': 'comment',
                            '_source': child,
                            '_id': str(event['ticket_id']) + '.' + str(child['id'])})
    return res


s = Search(using=es, index=index)
s.query = Q({'match': {'_id': 'event'}})
yesterday = s.execute().hits[0]['doc']['end_time']

result_generator = zendesk.tickets.events(start_time=yesterday)
ev_0 = result_generator.process_page()
n = len(ev_0)
actions = form_action_event(ev_0)
bulk(es, actions)
actions = []

if n < 1000:
    es.index(index=index, doc_type='config', id='event', body={'doc': {'end_time': result_generator.end_time}})

while n >= 1000:
    result_generator = zendesk.tickets.events(start_time=yesterday)
    events = result_generator.get_next_page()
    n = len(events['ticket_events'])
    actions = form_action_event(events['ticket_events'])
    if n >= 1000:
        yesterday = result_generator.end_time
    es.index(index=index, doc_type='config', id='event', body={'doc': {'end_time': result_generator.end_time}})
    print result_generator.end_time, bulk(es, actions)
