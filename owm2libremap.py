'''Pull data from an OpenWiFiMap db into a LibreMap db'''
import argparse
import couchdb
import json


def owm2libremap(owm_db_url, lm_db_url, continuous=False):
    '''Pull data from an OpenWiFiMap db into a LibreMap db

    Returns the update_seq in the OpenWiFiMap db
    '''
    owm_db = couchdb.client.Database(owm_db_url)
    lm_db = couchdb.client.Database(lm_db_url)
    print(owm_db.info())
    print(lm_db.info())


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
    config = json.load(file(args.config))
    print(args.continuous)
    owm2libremap(config['owmdb'], config['lmdb'], args.continuous)

if __name__ == '__main__':
    main()
