'''Pull data from an OpenWiFiMap db into a LibreMap db'''
import argparse
import couchdb
import json


def convert_doc(owm_doc):
    '''Convert a document from OpenWiFiMap format to LibreMap format'''
    from copy import deepcopy
    owm_doc = deepcopy(owm_doc)
    owm_handled_keys = []
    if 'type' not in owm_doc or owm_doc['type'] != 'node':
        return None

    new_doc = {
        '_id': 'owm2libremap_' + owm_doc['_id'],
        'api_rev': '1.0',
        'type': 'router',
        }
    owm_handled_keys = ['_id', '_rev', 'type']

    # map keys from owm to libremap
    keymap = {
        'hostname': 'hostname',
        'ctime': 'ctime',
        'mtime': 'mtime',
        'latitude': 'lat',
        'longitude': 'lon',
        'height': 'elev',
        }
    for owm_key, lm_key in keymap.iteritems():
        if owm_key in owm_doc:
            new_doc[lm_key] = owm_doc[owm_key]
            owm_handled_keys.append(owm_key)

    # move all unhandled data into 'attributes' of new_doc
    #for owm_key, val in owm_doc.iteritems():
    #    if owm_key not in owm_handled_keys:
    #        if 'attributes' not in new_doc:
    #            new_doc['attributes'] = {}
    #        new_doc['attributes'][owm_key] = val

    return new_doc


def owm2libremap(configfile, continuous=False):
    '''Pull data from an OpenWiFiMap db into a LibreMap db

    Returns the update_seq in the OpenWiFiMap db
    '''
    config = json.load(file(configfile))
    owm_db = couchdb.client.Database(config['owm_db'])
    owm_db_seq = config['owm_db_seq'] if 'owm_db_seq' in config else 0
    lm_db = couchdb.client.Database(config['lm_db'])

    count = 0
    while count < 1:
        # pull changes from owm
        changes = owm_db.changes(
            since=owm_db_seq,
            include_docs=True,
            feed='longpoll' if continuous else 'normal'
            )

        # convert owm documents to libremap documents
        new_docs = {}
        for change in changes['results']:
            new_doc = convert_doc(change['doc'])
            if new_doc is not None:
                new_docs[new_doc['_id']] = new_doc

        # get revisions from libremap
        lm_rows = lm_db.view('_all_docs', keys=new_docs.keys(),
                             include_docs=True)
        for lm_row in lm_rows:
            lm_doc = lm_row.doc
            if lm_doc is not None:
                new_docs[lm_doc['_id']]['_rev'] = lm_doc['_rev']

        # save to libremap
        upd = lm_db.update(new_docs.values())

        owm_db_seq = changes['last_seq']

        if continuous:
            # TODO: sleep
            pass
        else:
            count = count+1
        config['owm_db_seq'] = owm_db_seq
        json.dump(config, file(configfile, 'w'), indent=4)


def main():
    '''Parse arguments and call owm2libremap'''
    parser = argparse.ArgumentParser(
        description='Pull data from an OpenWiFiMap db into a LibreMap db'
        )
    parser.add_argument(
        '--config',
        metavar='FILE',
        default='config.json',
        required=True,
        help='config JSON file'
        )
    parser.add_argument(
        '--continuous',
        default=False,
        action='store_true',
        help='continuous updates'
        )
    args = parser.parse_args()
    owm2libremap(args.config, args.continuous)

if __name__ == '__main__':
    main()
