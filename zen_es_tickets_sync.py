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

def form_action_ticket(tickets):
    res = []
    metrics = []
    for ticket in tickets:
        if type(ticket) != dict:
            ticket = ticket.to_dict()
            via = ticket['via'].to_dict()
            via['source'] = via['source'].to_dict()
            ticket['via'] = via

        ticket['metric_events'].update(ticket['metric_set'])
        ticket['metric_events']['@timestamp'] = ticket['metric_set']['created_at']
        ticket['@timestamp'] = ticket['created_at']

        res.append({'_op_type': 'index',
            '_index': index,
            '_type': 'metric',
            '_source': ticket['metric_events'],
            '_id': ticket['id']
            })

        del ticket['metric_set']
        del ticket['metric_events']
        metrics.append({'_op_type': 'index',
            '_index': index,
            '_type': 'ticket',
            '_source': ticket,
            '_id': ticket['id']
            })
    return res, metrics

s = Search(using=es, index=index)
s.query =Q({'match': {'_id': 'ticket'}})
yesterday = s.execute().hits[0]['doc']['end_time']
result_generator = zendesk.tickets.incremental(start_time=yesterday)
ev_0 = result_generator.process_page()

actions, metrics  = form_action_ticket(ev_0)
bulk(es, actions + metrics)
n = len(ev_0)
actions = []

if n < 1000:
    end_time = result_generator.end_time
    es.index(index=index, doc_type='config', id='ticket', body={'doc': {'end_time': end_time}})

while n >= 1000:
    result_generator = zendesk.tickets.incremental(start_time=yesterday)
    tickets = result_generator.get_next_page()
    n = len(tickets['tickets'])
    actions, metrics = form_action_ticket(tickets['tickets'])
    if n >= 1000:
        yesterday = result_generator.end_time
    es.index(index=index, doc_type='config', id='ticket', body={'doc': {'end_time': result_generator.end_time}})
    print result_generator.end_time, bulk(es, actions + metrics)