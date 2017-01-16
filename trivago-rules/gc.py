from elastalert.ruletypes import RuleType
from elastalert.util import pretty_ts
from elastalert.util import hashable
from elastalert.util import lookup_es_key
from collections import defaultdict


class GCThroughput(RuleType):
    required_options = frozenset(['query_key', 'timeframe', 'availability'])

    servers = defaultdict(list)

    # From elastalert docs:
    #     add_data will be called each time Elasticsearch is queried.
    #     data is a list of documents from Elasticsearch, sorted by timestamp,
    #     including all the fields that the config specifies with "include"
    def add_data(self, data):

        def add_gc_event(qk, event_time, duration):
            self.servers[qk].append(GcData(event_time, duration))
            return

        # delete all times older than current - timeframe
        def remove_old_data(qk, event_time):
            if (qk in self.servers):
                for gcevent in self.servers[qk]:
                    if gcevent.timestamp < (event_time - self.rules['timeframe']):
                        self.servers[qk].remove(gcevent)
                    else:
                        break

        def get_availability(qk):
            total_time_in_ms = self.gc_time_sum(self.servers[qk])
            total_time_in_s = total_time_in_ms / 1000
            timeframe_in_s = float(self.rules['timeframe'].total_seconds())
            percent_available = (float(timeframe_in_s - total_time_in_s) / timeframe_in_s * 100)
            return percent_available
        
        def should_trigger(percent_available):
            return percent_available < self.rules['availability']

        for document in data:
            qk = self.rules.get('query_key')
            qk = hashable(lookup_es_key(document, qk))
            add_gc_event(qk, document['@timestamp'], document['duration'])
            remove_old_data(qk, document['@timestamp'])
            percent_available = get_availability(qk)
            if should_trigger(percent_available):
                self.add_match(document, qk, percent_available)

    def add_match(self, match, qk, percent_available):
        total_time_in_seconds = self.gc_time_sum(self.servers[qk]) / 1000
        extra_info = {'percent_available': percent_available, 'total_time_in_seconds': total_time_in_seconds, }
        match = dict(match.items() + extra_info.items())
        super(GCThroughput, self).add_match(match)

    # The results of get_match_str will appear in the alert text
    def get_match_str(self, matchedDocument):
        message = "Server has low availability:\n"
        message += "  service id: %d\n" % matchedDocument['serviceId']
        message += "  gc type: %s\n" % matchedDocument['collector']
        message += "  availability before %s was %f percent.\n" % (
            pretty_ts(matchedDocument['@timestamp'], self.rules.get('use_local_time')),
            matchedDocument['percent_available'])
        message += "Total time spent in GC: %d seconds" % matchedDocument['total_time_in_seconds']
        return message

    # From elastalert docs:
    # garbage_collect is called indicating that ElastAlert has already been run up to timestamp
    # It is useful for knowing that there were no query results from Elasticsearch because
    # add_data will not be called with an empty list
    def garbage_collect(self, timestamp):
        pass

    def gc_time_sum(self, gc_events):
        total_duration = 0
        for gc_event in gc_events:
            total_duration += gc_event.duration
        return total_duration


class GcData:
    def __init__(self, timestamp, duration):
        self.timestamp = timestamp
        self.duration = duration
