'''Pull data from an AlterMap db into a LibreMap db'''
import argparse
import couchdb
import json
from time import sleep, strftime, gmtime

def get_community_name(network_id, am_db):
    network = am_db.get(network_id)
    if network and 'name' in network:
        return network['name']

def convert_doc(am_doc, am_db):
    '''Convert a document from AlterMap format to LibreMap format'''
    from copy import deepcopy
    am_doc = deepcopy(am_doc)
    if 'collection' not in am_doc or am_doc['collection'] != 'nodes':
        return None

    current_time = strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime())

    new_doc = {
        '_id': am_doc['_id'],
        'api_rev': '1.0',
        'type': 'router',
        'hostname': am_doc['name'],
        'lat': am_doc['coords']['lat'],
        'lon': am_doc['coords']['lon'],
        'ctime': current_time,
        'mtime': current_time
        }

    if 'network_id' in am_doc:
        community_name = get_community_name(am_doc['network_id'], am_db)
        if community_name:
            new_doc['community'] = community_name

    return new_doc


def am2libremap(configfile, continuous=False):
    '''Pull data from an AlterMap db into a LibreMap db

    Returns the update_seq in the AlterMap db
    '''
    config = json.load(file(configfile))
    am_db = couchdb.client.Database(config['am_db'])
    am_db_seq = config['am_db_seq'] if 'am_db_seq' in config else 0
    lm_db = couchdb.client.Database(config['lm_db'])

    keep_going = True
    while keep_going:
        # pull changes from am
        changes = am_db.changes(
            since=am_db_seq,
            include_docs=True,
            feed='longpoll' if continuous else 'normal'
            )

        # convert am documents to libremap documents
        new_docs = {}
        for change in changes['results']:
            new_doc = convert_doc(change['doc'], am_db)
            if new_doc is not None:
                new_docs[new_doc['_id']] = new_doc

        # get revisions from libremap
        lm_rows = lm_db.view('_all_docs', keys=new_docs.keys(),
                             include_docs=True)
        for lm_row in lm_rows:
            lm_doc = lm_row.doc
            if lm_doc is not None:
                new_docs[lm_doc['_id']]['_rev'] = lm_doc['_rev']
                new_docs[lm_doc['_id']]['ctime'] = lm_doc['ctime']

        existing_lm_rows = lm_db.view('_all_docs', include_docs=False)

        upd = lm_db.update(new_docs.values())
        
        print "Updated"
        print upd

        am_db_seq = changes['last_seq']

        if continuous:
            sleep(10)
        else:
            keep_going = False

        config['am_db_seq'] = am_db_seq
        json.dump(config, file(configfile, 'w'), indent=4)


def main():
    '''Parse arguments and call am2libremap'''
    parser = argparse.ArgumentParser(
        description='Pull data from an AlterMap db into a LibreMap db'
        )
    parser.add_argument(
        '--config',
        metavar='FILE',
        default='config.json',
        required=False,
        help='config JSON file'
        )
    parser.add_argument(
        '--continuous',
        default=False,
        action='store_true',
        help='continuous updates'
        )
    args = parser.parse_args()
    am2libremap(args.config, args.continuous)

if __name__ == '__main__':
    main()
