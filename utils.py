import sys
import datetime
import pprint
import shutil

from StringIO import StringIO

import pymongo, gridfs, json
import fabric.api as fapi
import fabric.state as fstate
from fabric.context_managers import show, hide
from fabric.exceptions import NetworkError
from paramiko.rsakey import RSAKey
from paramiko.dsskey import DSSKey
from pyDes import triple_des
import base64
from bottle import template, HTTPResponse
from ordereddict import OrderedDict
import concurrent.futures as futures

class LocalEnv(dict):
    pass

fapi.env.warn_only = True

scripts_order = ['pre', 'deploy', 'selftest', 'version']

def uniqify(seq):
    res = []
    for i in seq:
        if i not in res:
            res.append(i)
    return res

def get_secret(filename):
    fh = open(filename)
    secret = fh.read()
    fh.close()
    return secret

def encrypt(data, secret):
    ciphertext = triple_des(secret).encrypt(data, padmode=2)
    return base64.b64encode(ciphertext)

def decrypt(b64_ciphertext, secret):
    ciphertext = base64.b64decode(b64_ciphertext)
    data = triple_des(secret).decrypt(ciphertext, padmode=2)
    return data

def is_authenticated(u, p):
        retval = False
        try: users = pymongo.Connection().sds.users
        except(pymongo.errors.AutoReconnect):
                print 'Couldn\'t connect to SDS db'
                return retval
        try: stored_password = users.find_one({'u_and_c': u})['password']
        except:
                print 'Unknown user'
                return retval
        secret = get_secret('keyfile')
        clear_password = decrypt(stored_password, secret)
        retval = p == clear_password
        return retval

def gridfs_save(file_handler, file_name):
        try:
            db = pymongo.Connection().sds
        except(pymongo.errors.AutoReconnect):
            print "Couldn't connect to SDS db\n"
        gfs = gridfs.GridFS(db)
        try:
            _id = gfs.put(file_handler, filename=file_name)
        except(pymongo.errors.AutoReconnect):
            _id = False
        return _id

def gridfs_get(_id):
        try:
            db = pymongo.Connection().sds
        except(pymongo.errors.AutoReconnect):
            print "Couldn't connect to SDS db\n"
        gfs = gridfs.GridFS(db)
        try:
            retval = gfs.get(_id)
        except(gridfs.errors.NoFile):
            retval = False
        return retval

def get_sds_model(modelname, company):
        try:
            models = pymongo.Connection().sds.models
        except(pymongo.errors.AutoReconnect):
            return False
        try:
            model = models.find_one({'company': company, 'env': modelname})['model']
        except:
            return False

        return model

def validate_model(model):
    if not model.has_key('name'):
         raise HTTPResponse('A model MUST have a name field\n', 400)
    if type(model['name']) != unicode or len(model['name']) == 0:
        raise HTTPResponse(
            'The name value MUST be a non zero length string\n', 400)
    if not model.has_key('arts_to_nodes') or len(model['arts_to_nodes']) == 0:
        raise HTTPResponse(
            'A model MUST have an arts_to_nodes field with a non-zero length\n',
                400)
    for art in model['arts_to_nodes']:
        if type(model['arts_to_nodes'][art]) != list:
            raise HTTPResponse(
                'The values of arts_to_nodes map MUST be of list type\n', 400)
    return True

def stringify(obj):
    '''Takes a python object and returns its string representation'''
    if type(obj) == list:
        return '\n'.join(obj) + '\n'
    elif type(obj) == dict:
        return json.dumps(obj, indent=4) + '\n'
    else:
        return str(obj) + '\n'
    
def get_status(token, no_id=False):
    '''Gets an id and returns a status dict associated with it or None
    If no_id default parameter is true, returns the status without the "_id"'''
    try:
        status = pymongo.Connection().sds.status
        if no_id:
            return status.find_one({'_id': token}, {'_id': 0})
        else:
            return status.find_one({'_id': token})
    except(pymongo.errors.AutoReconnect):
        print "Couldn't connect to SDS db\n"

def get_logs(token):
    '''Gets a deployment id and returns its logs object or None'''
    try:
        logs = pymongo.Connection().sds.logs
        return logs.find_one({'_id': token})
    except(pymongo.errors.AutoReconnect):
        print "Couldn't connect to SDS db\n"

def update_logs(log_obj):
    '''Gets a log object. Saves the log_obj as a new object and returns its id
    if no id was embedded in it or updates existing object. Returns obj id if
    no error occured, False otherwise'''
    try:
        logs = pymongo.Connection().sds.logs
        oid = logs.save(log_obj, safe=True)
        return oid
    except(pymongo.errors.AutoReconnect):
        print 'Couldn\'t connect to SDS db'
        return False

def update_status(stat_dict):
    '''Gets a status dict. Saves the status as a new object and returns its id
    if no id was embedded in it or updates existing object. Returns obj id if
    no error occured, False otherwise'''
    try:
        status = pymongo.Connection().sds.status
        oid = status.save(stat_dict, safe=True)
        return oid
    except(pymongo.errors.AutoReconnect):
        print 'Couldn\'t connect to SDS db'
        return False

def gen_copy_steps(model, key, token, art, node=None):
    ''' Generates the copying steps. Each step consists of 1 or more funcs
    with arguments stored as lists. Called by generate_steps.
    If node arg is given, will generate steps only for this node, otherwise
    will generate copy steps for all relevant nodes in the model.
    '''
    step = []
    if not node:
        for node in model['arts_to_nodes'][art['type']]:
            step.append([copy_art, art, node, key, token])
    else:
        step.append([copy_art, art, node, key, token])
    return step

def gen_script_steps(model, key, token, art, company, node=None):
    ''' Generates the script steps. Each step consists of 1 or more funcs
    with arguments stored as lists. Called by generate_steps.
    If node arg is given, will generate steps only for this node, otherwise
    will generate script steps for all relevant nodes in the model.
    Unlike gen_copy_steps returns an OrderedDict instance instead of list.
    its return value must be added and not appended. A usage example:

    for s_type in script_steps:
        steps.append(script_steps[s_type])
    '''
    if 'arts_to_scripts' not in model.keys(): # No scripts in this model
        return None
    elif art['type'] not in model['arts_to_scripts'].keys(): # No scripts for art
        return None
    else:
        if not node:
            node_list = model['arts_to_nodes'][art['type']]
        else:
            node_list = [node]

        env = model['name']
        # Add correct order to the script steps
        s_type_dict = OrderedDict()
        script_types = model['arts_to_scripts'][art['type']].keys()
        sorted_script_types = []
        for s_type in scripts_order:
            if s_type in script_types:
                sorted_script_types.append(s_type)
        
        for s_type in sorted_script_types:
            s_type_list = []
            for node in node_list:
                s_type_list.append([run_script, s_type, art, node, env,
                    company, key, token])
            s_type_dict[s_type] = s_type_list
            
    return s_type_dict

def generate_steps(model, key, token, company, **arts):
    '''Gets an environment model, key, status token and a dict of artifacts.
    The arts dict looks like this:
    {'art_type': {'file_path': string, 'filename': string, 'type': string}}
    Returns a scenario or None if error occured.
    A scenario is a list of steps. Each step has one or more function(s) with
    argument(s).  The steps are executed in sequence, but the functions in each
    step are executed in parallel.'''
    steps = []

    if 'order' in model.keys():
        for step in model['order']:
            if type(step) == unicode:  # Add stuff for this artifact
                art_type = step
                if art_type in arts:
                    steps.append(gen_copy_steps(model, key, token,
                        arts[art_type]))
                    script_steps = gen_script_steps(model, key, token,
                        arts[art_type], company)
                    if script_steps:
                        for s_type in script_steps:
                            steps.append(script_steps[s_type])
            elif type(step) == list:  # Add stuff for all the arts in the list
                copy_step = []
                for art_type in step:
                    if art_type in arts:
                        copy_step = copy_step + gen_copy_steps(model, key, token,
                            arts[art_type])
                if len(copy_step) > 0:
                    steps.append(copy_step)

                script_step = []
                for art_type in step:
                    if art_type in arts:
                        script_steps = gen_script_steps(model, key, token,
                            arts[art_type], company)
                        if script_steps: script_step.append(script_steps)
                #
                ## In a special case where the list has only 1 member we
                ## emulate the 'unicode' behaviour
                #

                if len(script_step) == 1:
                    script_steps = script_step[0]
                    for s_type in script_steps:
                        steps.append(script_steps[s_type])
                    steps = steps + script_step

                else: # Adding the same script types to previous steps. 
                    unified_dict = {}
                    for phase in script_step:
                        for s_type in phase:
                            if s_type not in unified_dict:
                                unified_dict[s_type] = phase[s_type]
                            else:
                                unified_dict[s_type] = (unified_dict[s_type] +
                                    phase[s_type])
                    # Adding the keys in right order
                    
                    for s_type in scripts_order:
                        if s_type in unified_dict:
                            steps.append(unified_dict[s_type])
                    #pprint.pprint (unified_dict, indent=4)
                
            elif type(step) == dict:  # Add only for the given art-node pairs
                art_type = step.keys()[0]   
                if art_type in arts:
                    steps.append(gen_copy_steps(model, key, token, arts[art_type],
                        step[art_type]))
                    script_steps = gen_script_steps(model, key, token,
                        arts[art_type], company, step[art_type])
                    if script_steps:
                        for s_type in script_steps:
                            steps.append(script_steps[s_type])
            else:
                raise  HTTPResponse("Don't know what to do for %s in order.\n"
                    % type(step), 400)
    else: # A model without order attribute
        step = []
        for art in model['arts_to_nodes']:
            if art in arts:
                for node in model['arts_to_nodes'][art]:
                    step.append([copy_art, arts[art], node, key, token])
        steps.append(step)

        if 'arts_to_scripts' in model.keys():
            script_step = []
            for art in model['arts_to_nodes']:
                if art in arts:
                    script_steps = gen_script_steps(model, key, token,
                        arts[art], company)
                    if script_steps:
                        script_step.append(script_steps)

            if len(script_step) == 1:
                    script_steps = script_step[0]
                    for s_type in script_steps:
                        steps.append(script_steps[s_type])

            else: # Adding the same script types to previous steps.
                unified_dict = {}
                for phase in script_step:
                    for s_type in phase:
                        if s_type not in unified_dict:
                            unified_dict[s_type] = phase[s_type]
                        else:
                            unified_dict[s_type] = (unified_dict[s_type] +
                                phase[s_type])

                # Adding the keys in right order
                for s_type in scripts_order:
                    if s_type in unified_dict:
                        steps.append(unified_dict[s_type])
        
    return steps

def run_steps(dep_steps, company, token, dry_run=False):
    '''Gets the list of steps from generate_steps function and starts the actual
    deployment. Returns the URL of deployment status or None if error occured.
    If dry_run arg is specified, returns a printable list of deployment steps
    instead of running them.
    '''

    if dry_run: # Don't do anything, pretty print the steps
        try:
            return template('/home/simplds/sds/srv/show_dep_phasenames.tpl',
                dep_steps=dep_steps)
        except:
            print sys.exc_info()
            return pprint.pformat(dep_steps, indent=4)
    else:        
        print 'deployment started', str(datetime.datetime.now())
        sys.stdout.flush()
        for step in dep_steps:
            step_name = gen_step_name(step)
            status_object = get_status(token)
            # Stop execution and return status immediately on error
            if status_object['Error occured']:
                print 'deployment ended', str(datetime.datetime.now())
                sys.stdout.flush()
                return False
            status_object['running step'] = step_name
            update_status(status_object)
            for s_step in step:
                # Run individual functions under each steps simultaneously
                mw = len(s_step) # Setting up the maximum workers...
                with futures.ThreadPoolExecutor(max_workers=mw) as executor: 
                    
                    # Initializing the futures pool and adding all the substep
                    # functions to executor.
                    substep = []
                    substep.append(executor.submit(s_step[0], *s_step[1:]))
                    # Waiting for all the futures in the pool to complete
                    for future in futures.as_completed(substep):
                        if future.exception() is not None:
                            print 'exception', future.exception()
                            sys.stdout.flush()
                        else: # Print log if error occured
                            if not future.result()[0]:
                                print cat_gfile(future.result()[1])
                                sys.stdout.flush()

        # Clean up the arts directory after deployment
        dn = '/tmp/deployment_arts/%s/%s/' % (company, str(token))
        shutil.rmtree(dn)
        # Mark the deployment status finished.
        status_object['Deployment finished'] = True
        update_status(status_object)
        print 'deployment ended', str(datetime.datetime.now())
        sys.stdout.flush()
        return True

def nice_stringify(seq):
    '''
    Gets a sequence of items and returns a tuple of '' or 's' and 
    gramattically correct list, like this: ('s', 'item1, item2 and item3')
    '''
    if len(seq) == 1:
        return ('', seq[0])
    else:
        ret_str = ', '.join(seq[:-1]) + ' and %s' % seq[-1]
        return ('s', ret_str)
        

def gen_step_name(step):
    if len(step) == 1:
        if step[0][0].func_name == 'copy_art':
            return 'Copying %s to %s' % (step[0][1]['type'], step[0][2])
        elif step[0][0].func_name == 'run_script':
            return 'Running script %s %s on %s' %(step[0][1], step[0][2]['type'],
                step[0][3])
        else:
            return 'Running function %s with arguments %s' % (step[0][0],
                step[0][1:])
    else: # Multiple functions running simultaneously
        if step[0][0].func_name == 'copy_art':
            arts = []
            nodes = []
            for func in step:
                arts.append(func[1]['type'])
                nodes.append(func[2])
            arts = uniqify(arts)
            nodes = uniqify(nodes)
            return  ('Copying artifact%s %s' % nice_stringify(arts) + 
                ' to node%s %s' % nice_stringify(nodes))
        elif step[0][0].func_name == 'run_script':
            nodes = []
            for func in step:
                nodes.append(func[3])
            nodes = uniqify(nodes)
            return ('Running script %s' % step[0][1] +
                ' on node%s %s' % nice_stringify(nodes))

def copy_art(art, node, key, token):
    '''Tries to copy the artifact to node. The argument art is a dict
    of {'file_path': file, 'filename': 'string', 'type': string}
    Returns a tuple of (Bool, obj_id) with the completion status and log
    file object handler. Updates the status using provided token.'''
    # Make a new gfs file, update the deployment logs object and redirect
    # all output to this file
    db = pymongo.Connection().sds
    gfs = gridfs.GridFS(db)
    try:
        logfile = gfs.new_file()
    except(pymongo.errors.AutoReconnect):
        print 'Error while working with gridfs.\n'
        return (False, None)
    log_id = logfile._id
    log_obj = get_logs(token)
    func_id = ('copy_%s_%s' % (art['type'], node)).replace('.', '_dot_')
    log_obj[func_id] = log_id
    update_logs(log_obj)
    log_id = logfile._id
    redirect_sys_output(logfile)
    status_object = get_status(token)
    # Check if the remote host is reachable by SSH
    can_connect = get_output('uname', node, key)
    if not can_connect:
        print "Can't connect to %s." % node
        status_object['Error occured'] = True
        update_status(status_object)
        restore_sys_output(logfile)
        return (False, log_id)
    fapi.env.host_string = node
    fapi.env.pkey = key
    fapi.env.warn_only = True
    fapi.env.user = 'root'

    r_filename = '/tmp/%s' % (art['filename'])

    try:
        stat = fapi.put(art['file_path'], r_filename).succeeded
        restore_sys_output(logfile)
    except:
        stat = False
        restore_sys_output(logfile)
        status_object['Error occured'] = True
        update_status(status_object)
    if not stat:
        status_object['Error occured'] = True
        update_status(status_object)
    return (stat, log_id)

def run_script(script, art, node, env, company, key, token):
    '''Runs the script string remotely using fabric api. Redirects stderr/stdout
    to a gridfs file. Returns a tuple of (Bool, obj_id) with the completion
    status and log file object handler. Updates the status using provided token.
    '''
    # Make a new gfs file, update the deployment logs object and redirect
    # all output to this file
    db = pymongo.Connection().sds
    scripts = db.scripts
    gfs = gridfs.GridFS(db)
    try:
        logfile = gfs.new_file()
    except(pymongo.errors.AutoReconnect):
        print 'Error while working with gridfs.\n'
        return (False, None)
    log_id = logfile._id
    log_obj = get_logs(token)
    func_id = ('%s_%s_%s' % (script, art['type'], node)).replace('.', '_dot_')
    log_obj[func_id] = log_id
    update_logs(log_obj)
    redirect_sys_output(logfile)
    status_object = get_status(token)
    # Check if the remote host is reachable by SSH
    can_connect = get_output('uname', node, key)
    if not can_connect:
        print "Can't connect to %s." % node
        restore_sys_output(logfile)
        status_object['Error occured'] = True
        update_status(status_object)
        return (False, log_id)
    # Get the script contents from the db and try to run the script
    fapi.env.host_string = node
    fapi.env.pkey = key
    fapi.env.warn_only = True
    script_str = scripts.find_one({'company': company, 'env': env,
        'type': script, 'art': art['type']})['str']
    if script == 'deploy':  # Export art_path before running the script
        path = '/tmp/%s' % art['filename']
        script_str = 'export art_path=%s; %s' % (path, script_str)
    try:
        stat = fapi.run(script_str).succeeded
        restore_sys_output(logfile)
    except:
        stat = False
        restore_sys_output(logfile)
        status_object['Error occured'] = True
        update_status(status_object)
    if not stat:
        status_object['Error occured'] = True
        update_status(status_object)
    return (stat, log_id)
         
def redirect_sys_output(file_obj):
    '''Redirects stdout to a file object. Returns original output handler
    if the file_obj is writable or False otherwise'''
    try:
        mode = file_obj.mode
    except:
        if type(file_obj) == gridfs.grid_file.GridIn:
            mode = 'w'
        else:
            print '%s is not a file.\n' % file_obj
            return False
    if mode not in ['w', 'a', 'r+']:
        print '%s is not writable.\n' % file_obj
        return False
    stdout = sys.stdout
    if not hasattr(file_obj, 'isatty'):
        file_obj.isatty = lambda: False
    if not hasattr(file_obj, 'flush'):
        file_obj.flush = lambda: True
    if not hasattr(file_obj, 'encoding'):
        file_obj.encoding = 'UTF-8'
    sys.stdout = file_obj
    sys.stderr = file_obj
    return stdout

def restore_sys_output(file_obj):
    'Restores sys.std* to original values and closes the log file handler'
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    try:
        file_obj.close()
    except:
        print "Couldn't close output\n"

def get_output(command, node, key):
    'Returns the output of command run as root on a given node or False.'
    fapi.env.host_string = 'root@%s' % node
    fapi.env.pkey = key
    with hide('running', 'stdout'):
        try:
            res = fapi.run(command)
            return res
        except(NetworkError):
            print 'Bad host'
            return False

def str_to_key(key_str):
    '''Gets a string with rsa/dsa private key and converts it to paramiko key
    object. Returns RSAKey/DSSKey object if succeeded or None if error occured.'''
    if 'BEGIN RSA' in key_str:
        str_obj = StringIO(key_str)
        try:
            k = RSAKey(file_obj=str_obj)
            return k
        except:
            return None
    elif 'BEGIN DSA' in key_str:
        str_obj = StringIO(key_str)
        try:
            k = DSSKey(file_obj=str_obj)
            return k
        except:
            return None
    else:
        return None

def cat_gfile(file_id):
    'Gets an object id of a gridfs file and returns its content or None'
    db = pymongo.Connection().sds
    gfs = gridfs.GridFS(db)
    try:
        file_obj = gfs.get(file_id)
        return file_obj.read()
    except (gridfs.errors.NoFile):
        print "Couldn't open object %s - no such file in gridfs." % file_id
        return None

def str_to_obj(id_str):
    from bson.objectid import ObjectId
    if not id_str:
        return None
    try:
        obj = ObjectId(id_str)
    except:
        obj = None
    return obj

