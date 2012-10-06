#!/usr/bin/env python

import json
import time
import datetime
import os
import shutil
import subprocess
from bson.objectid import ObjectId

from bottle import (get, post, put, delete, request, run, debug, parse_auth, hook,
    HTTPError, HTTPResponse, default_app)
import pymongo
import gridfs

import utils

@hook('before_request')
def br():
    a_token = request.headers.get('Authorization')
    try: credentials = parse_auth(a_token)
    except:
        raise HTTPResponse('Please provide a Basic Authentication header\n', 403)
    if not utils.is_authenticated(*credentials): raise HTTPResponse(
        'Unauthenticated\n', 403)
    u_and_c = credentials[0].split('__')
    user = u_and_c[0]
    if len(u_and_c) >1:
        company = u_and_c[1]
    else:
        company = user
    request.creds = utils.LocalEnv()
    request.creds.user = user
    request.creds.company = company
    request.creds.password = credentials[1]

@get('/')
def print_routes():
    '''
    Kinda HATEOAS ;).
    See http://article.gmane.org/gmane.comp.python.bottle.general/480/match=routes+list
    curl localhost:8081/
    '''
    routes = []
    for route in default_app().routes:
        routes.append('%s %s' % (route.method, route.rule))
    return utils.stringify(routes)

@post('/environments')
def post_model():
    '''
    curl --form env=@dev1.jsn localhost:8081/environments
    '''
    try:
        models = pymongo.Connection().sds.models
    except(pymongo.errors.AutoReconnect):
        raise HTTPResponse('Couldn\'t connect to SDS db\n', 500)
    try:
        model = json.loads(request.files.env.value)
    except:
        raise HTTPResponse('You need to upload a valid json file\n', 400)
    if not utils.validate_model(model):
        raise HTTPResponse('Your model is invalid\n', 400)
    c = request.creds
    user, company = c.user, c.company
    model_obj = {'user': user, 'company': company, 'env': model['name'],
        'model': model}
    models.update({'company': company, 'env': model['name']}, model_obj, upsert=True)

@delete('/environments')
def delete_model():
    '''
    curl -X DELETE localhost:8081/environments?env=dev3
    '''
    try:
        models = pymongo.Connection().sds.models
    except(pymongo.errors.AutoReconnect):
        raise HTTPResponse('Couldn\'t connect to SDS db\n', 500)
    company = request.creds.company
    env = request.query.get('env')
    if env == None:
        raise HTTPResponse('Please provide the model name\n', 400)
    models.remove({'company': company, 'env': env})

@get('/environments')
def get_model():
    '''
    curl localhost:8081/environments?env=dev1 or
    curl localhost:8081/environments
    '''
    try:
        models = pymongo.Connection().sds.models
    except(pymongo.errors.AutoReconnect):
        raise HTTPResponse('Couldn\'t connect to SDS db\n', 500)
    company = request.creds.company
    env = request.query.get('env')
    if not env: # Will list all models for the given company
        models = [m['env'] for m in models.find({'company': company})]
        if len(models) == 0:
            return 'You have no models\n'
        return utils.stringify(models)
    else:
        try:
            model = models.find_one({'company': company, 'env': env})['model']
        except:
            raise HTTPResponse('Model %s not found\n' % env, 404)
        return utils.stringify(model)

@post('/scripts')
def save_scripts():
    '''
    curl -F pre=@art1_pre.sh -F deploy=@art1_deploy.sh\
    'localhost:8081/scripts?artifact=art1&env=dev1'
    '''
    artifact, env = request.query.get('art'), request.query.get('env')
    if None in (artifact, env): raise HTTPResponse(
        'Please provide artifact and environment name\n', 400)
    company = request.creds.company
    model = utils.get_sds_model(env, company)
    if model:
        artifacts = model['arts_to_nodes'].keys()
    else:
        raise HTTPResponse('Couldn\'t get model %s\n' % env, 500)
    if artifact not in artifacts: raise HTTPResponse(
        'Artifact %s is not supported in model %s\n' % (artifact, env), 400)
    script_types = [s for s in request.files]
    if len(script_types) == 0: raise HTTPResponse(
        'You need to upload at least one script type\n', 400)
    try:
        sds = pymongo.Connection().sds
    except(pymongo.errors.AutoReconnect):
        raise HTTPResponse('Couldn\'t connect to SDS db\n', 500)
    scripts = sds.scripts
    script_dict = {}
    
    allowed_types = ['pre', 'deploy', 'selftest', 'version']
    for t in script_types:
        if t not in allowed_types:
            raise HTTPResponse(
                'Sorry, script type %s is not supported\n' % t, 400)
        script_object = {'company': company, 'env': env, 'art': artifact,
            'str': request.files[t].file.read(),
            'fn': request.files[t].filename, 'type': t}
        script_template = {'company': company, 'env': env, 'art': artifact,
            'type': t}
        script_id = scripts.update(script_template, script_object, upsert=True,
            safe=True)
        if script_id:
            script_dict[t] = request.files[t].filename
        else:
            raise HTTPResponse('Couldn\'t save the script %s\n' %
                request.files[t].filename, 500)

    if not model.has_key('arts_to_scripts'): # Never attached any scripts to model
        a2s = {artifact: script_dict} # Initializing with what we have
    elif not model['arts_to_scripts'].has_key(artifact): # No scripts for current art
        a2s = model['arts_to_scripts'] # Copying the existing scripts... 
        a2s[artifact] = script_dict # And adding the new ones.
    else: # Current art has scripts
        a2s = model['arts_to_scripts']
        for k in script_dict:
            a2s[artifact][k] = script_dict[k]
    
    model['arts_to_scripts'] = a2s
    models = sds.models
    models.update({'company': company, 'env': env}, {'$set': {'model': model}})
            
@post('/deploy')
def deploy_env():
    '''Extracts at least 1 file with an artifact to deploy from request.files
    and an ssh key from request.forms.key.
    Starts a deployment process for the environment specified in the 'env' URL
    parameter and returns a url with the deployment status or an error message
    if the deployment couldn't start.

    curl -F "art2=@curlMan" -F "key=<.ssh/id_dsa_" localhost:8081/deploy?env=dev4
    '''
    env = request.query.get('env')
    if not env:
        raise HTTPResponse('Please provide your environment name\n', 400)
    company = request.creds.company
    model = utils.get_sds_model(env, company)
    if not model:
        raise HTTPResponse('Couldn\'t find environment named %s\n' % env, 404)
    if len(request.files) == 0:
        raise HTTPResponse('Please upload at least 1 artifact\n', 400)
    if not request.forms.key:
        raise HTTPResponse(
            'Please upload the contents of your ssh private key in a "key" form field\n', 400)
    pkey = utils.str_to_key(request.forms.key)
    if not pkey:
        raise HTTPResponse("Couldn't decode your SSH key.\n", 400)
    dry_run = request.query.get('dry_run')
    
    token = utils.update_status({'Error occured': False, 'Deployment finished': False})
    log_obj = utils.update_logs({'_id': token})

    arts_dict = {}
    for art in request.files:
        fs = request.files[art]
        if fs.name not in model['arts_to_nodes'].keys():
            raise HTTPResponse('Artifact %s is not supported in your model.\n' %
                fs.name, 400)
        # We need to save the artifacts in real temp files so that fapi.put
        # could work with them
        dn = '/tmp/deployment_arts/%s/%s/' % (company, str(token))
        fn = fs.name
        if not os.path.exists(dn):
            os.makedirs(dn)
        try:
            fh = open(dn+fn, 'wb')
            fh.write(fs.file.read())
            fh.close()
        except:
            raise HTTPResponse('Error occured while saving %s \n' % art, 500)
        arts_dict[fs.name] = {'type': fs.name, 'filename': fs.filename, 'file_path': dn+fn } 

    # Saving everything needed for generating and running steps in the db
    keys = pymongo.Connection().sds.keys
    args = pymongo.Connection().sds.args
    gen_step_args = {'model': model, 'token': token, 'company': company, 'arts': arts_dict}


    steps = utils.generate_steps(model, pkey, token, company, **arts_dict)
    if steps:
        if dry_run:
            shutil.rmtree(dn)
            return utils.run_steps(steps, company, token, dry_run=True)
        else:
            # This runs the deployment in a separate daemonized process:
            keys.save({'_id': token, 'key': request.forms.key})
            args.save({'_id': token, 'args': gen_step_args})
            p = subprocess.Popen(['/home/simplds/sds/srv/executor.py', str(token)])

            serverpart = 'https://sds.simplcloud.com'
            status_url = '"%s/status?token=%s&human_readable=true"' % (serverpart,
                str(token))
            log_url = '%s/logs/%s' % (serverpart, str(token))
            #return '%s\n%s\n' % (status_url, log_url)
            return ('"https://sds.simplcloud.com/status?token=%s&human_readable=true"\n' %
                str(token))
    else:
         raise HTTPResponse('Error occured while generating deployment steps.\n',
            500)
    
@get ('/status')
def get_status():
    '''
    Returns either a status object or its text represenation when given a valid
    token string.
    curl localhost:8081:/status?token=4f7b38cf02f0ba5c38000000
    curl 'localhost:8081:/status?token=4f7b38cf02f0ba5c38000000&human_readable=true'
    '''
    token = utils.str_to_obj(request.query.get('token'))
    if not token:
        raise HTTPResponse('Please specify a valid token.\n', 400)

    # Output formatting
    status = utils.get_status(token, no_id=True)
    human_readable = request.query.get('human_readable')
    if status:
        if human_readable: # Return text depending on the deployment's status
            if status['Deployment finished']:
                return 'Deployment finished.\n'
            elif status['Error occured']:
                return 'Error occured during deployment.\n'
            else:
                try:
                    return '%s.\n' % status['running step']
                except KeyError:
                    return 'Error occured before the beginning of deployment.\n'
        else: # Just return the whole status object
            return utils.stringify(status)
    else:
        raise HTTPResponse('No status found for this token\n', 404)

@get ('/logs/:token')
def get_logs(token):
    '''
    Returns all the logs associated with the given deployment token.
    curl localhost:8081/logs/4f8d3ebb02f0ba5cbb000000
    '''
    try:
        logs = pymongo.Connection().sds.logs
    except(pymongo.errors.AutoReconnect):
        raise HTTPResponse('Couldn\'t connect to SDS db\n', 500)

    log_id = ObjectId(token)
    ret_str = ''

    for log_file in logs.find_one({"_id": log_id}, {"_id": 0}).values():
        try:
            ret_str = ret_str + utils.cat_gfile(log_file)
        except(TypeError):
            print 'Got NotString'
            ret_str = ret_str + str(utils.cat_gfile(log_file))

    return ret_str


if __name__ == '__main__':
    # Development mode
    run(host='0.0.0.0', port=8081, reloader=True)

elif  __name__ == 'bottle_app':
    # Production mode
    debug = True
    application = default_app()
