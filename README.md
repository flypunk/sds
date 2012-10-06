SDS
===

SDS stands for Simple Deployment Service.
It's an HTTP API for doing deployments to SOA apps - see http://simplcloud.com

The API manual is at http://simplcloud.com/docs/API.html

Installation - TL;DR version

Install Mongodb on your machine and start it.

virtualenv sds
cd sds
source bin/activate
git clone https://github.com/flypunk/fabric.git
(cd fabric; python setup.py install)
pip install bottle
pip install pyDes
pip install ordereddict
pip install futures

git clone https://github.com/flypunk/sds.git

cd sds
./add_user.py Username Organization Password #Copy output of this command

# Running it:
./bottle_app.py

# Trying it:
# In a different terminal:
curl -H 'Authorization: Basic Output_of_the_add_user_command' localhost:8081/

