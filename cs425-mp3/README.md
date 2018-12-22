# cs425-mp3
We have used .ssh to make sure that we do not need to input password again during put and get.

## start
python3 setup.py

## command line
join: send request to join

leave: leave the system voluntary

lm: list members address

vm: list self's address

put localfilename sdfsfilename: to put local file to sdfs

get sdfsfilename localfilename: get file from sdfs

delete sdfsfilename: delete file in all replicas

ls sdfsfilename: show which vms store this file

store: show what this vm stores

get-versions sdfsfilename number localfilename: get n versions and merge them to one local file from sdfs
