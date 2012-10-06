#!/usr/bin/env python
'''
This script receives a mongo id as its argument. It retrieves a private key
and deployment args from the database, generates the deployment steps
and runs a deployment. The deployment process runs in daemonised mode,
i. e. it is not connected to a controlling terminal.
All the output from the deployment process is collected in mongo status
document and gridfs logs.
'''
import sys
from bson.objectid import ObjectId

import pymongo, gridfs
import daemon

from utils import *

def get_key(doc_id):
    '''Extracts a key from a mongo document and removes the document'''
    try:
        oid = ObjectId(doc_id)
    except:
        print 'Invalid doc_id'
        return None
    try:
        keys = pymongo.Connection().sds.keys
    except(pymongo.errors.AutoReconnect):
        print 'Problem connecting to the database'
        return None
    key = keys.find_one({'_id': oid}, {'_id': 0, 'key': 1})
    if not key:
        print 'Document %s not found' % doc_id
        return None
    else:
        retval = key['key']
        keys.remove({'_id': oid})
        return retval

if __name__ == '__main__':
    token = sys.argv[1]
    token_obj = ObjectId(token)
    pkey = str_to_key(get_key(token))
    args = pymongo.Connection().sds.args
    gen_step_args = args.find_one({'_id': token_obj})['args']

    model = gen_step_args['model']
    arts = gen_step_args['arts']
    company = gen_step_args['company']
    
    args.remove({'_id': token_obj})
    
    steps = generate_steps(model, pkey, token_obj, company, **arts)
    
    # Save potential errors to /tmp/mylog for debugging
    fd = open('/tmp/mylog', 'w+b')
    with daemon.DaemonContext(stdin=None, stdout=fd, stderr=fd):
        result = run_steps(steps, company, token_obj)
        if not result:
            print 'result', result
        fd.close()
