==============================================
fsdfs : f***ing simple distributed file system
==============================================



Goals
-----

fsdfs was originally developed at Jamendo.com to manage the distribution of millions of MP3/OGG files.

fsdfs aims to :
 * make a single virtual filesystem accessible from several nodes
 * be fully distributed (no single point of failure)
 * automatically manage sharding/replication : adding a new node takes 10 seconds
 * try to fill up its allowed disk quota to maximize redundancy
 * be extensible via simple python code
 * allow userland variables (like popularity) to boost a file's replication/redundancy level
 * sit on top of any POSIX filesystem

Roadmap
-------

For simplicity, fsdfs 1.0 will have a master/slaves design (slaves being readable even if the master is down).
 
fsdfs 2.0 will remove the need of a master node, possibly using a DHT network like Kademlia. Code for 1.0 will
already be structured to make its implementation easy.


Design for version 1.0
----------------------

Each file has two important variables :
* Its target replication count N. (N=3 by default, can be customized per-file by a user-defined python function)
* Its current replication count K
 
The master has a list of all the nodes with their status, which includes :
* cpu/network load
* list of all the files replicated on the node
* free disk quota
 
The free disk quota will always tend to zero because each nodes tries to replicate as many files as it can. 
Hence, max(K-N) is a more interesting number for a node, because it measures the extent of "over-replication".
If max(K-N)>0, some copies on the server are not strictly necessary and could be deleted. 
If max(K-N)<=0, every copy is needed and the server should be considered "full".

The primary job of the master is to manage file replication, making sure target replications counts are
met or exceeded. In other words, it has to make min(K-N) accross all known files as high as it can.

The algorithm used is very simple : 

1. select the file having min(K-N)
2. choose a node :
  * not already having a copy of this file
  * not too loaded
  * with either free disk or a high max(K-N)
3. Have the file copied to this node (=> K++)
4. GOTO 1


min(K-N) across all files is a good measure of the health and general disk usage of the filesystem. min(K-N) should be
positive, ideally greater than 1 (in which case all the files would have one more copy than the target replication count)

min(K-N) will naturally decrease when adding new files. min(K-N) becoming <1 or negative should be the signal that new nodes are needed.

How to test
-----------

	python tests/basic.py
	python tests/quota.py


.. include:: TODO.rst


Installation
------------

The easiest way to get fsdfs is if you have setuptools_ installed::

	easy_install fsdfs

Without setuptools, it's still pretty easy. Download the fsdfs.tgz file from 
`fsdfs's Cheeseshop page`_, untar it and run::

	python setup.py install

.. _fsdfs's Cheeseshop page: http://pypi.python.org/pypi/fsdfs/
.. _setuptools: http://peak.telecommunity.com/DevCenter/EasyInstall


Help and development
====================

If you'd like to help out, you can fork the project
at http://github.com/sylvinus/fsdfs and report any bugs 
at http://github.com/sylvinus/fsdfs/issues.


