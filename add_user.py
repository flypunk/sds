#!/usr/bin/env python

import sys
import base64

import pymongo

import utils

def generate_secret(length):
    import string
    import random
    chars = string.ascii_letters + string.digits
    l = []
    for i in range(length):
        l.append(random.choice(chars))
    return ''.join(l)

def generate_keyfile():
    fh = open('keyfile', 'w')
    fh.write(generate_secret(24))
    fh.close()

def add_user(user, company, password):
    retval = False
    try:
        users = pymongo.Connection().sds.users
    except:
        print 'Couldn\'t connect to SDS db'
        return retval
    secret = utils.get_secret('keyfile')
    enc_password = utils.encrypt(password, secret)
    o = {'password': enc_password, 'u_and_c': user+'__'+company}
    users.insert(o)
    encoded = base64.b64encode('%s__%s:%s' % (user, company, password))
    return encoded

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print 'Please enter username, company and password'
        sys.exit(1)
    import os.path
    if not os.path.isfile('keyfile'):
        generate_keyfile()
    user, company, password = sys.argv[1], sys.argv[2], sys.argv[3]
    print add_user(user, company, password)

