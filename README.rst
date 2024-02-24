=====
ml4ms
=====

.. image:: https://github.com/billingegroup/ml4ms/actions/workflows/testing.yml/badge.svg
   :target: https://github.com/billingegroup/ml4ms/actions/workflows/testing.yml


.. image:: https://img.shields.io/pypi/v/ml4ms.svg
        :target: https://pypi.python.org/pypi/ml4ms


Python package for facilitating machine learning tasks on collections of materials data, especially including measured spectra.

* Free software: 3-clause BSD license
* Documentation: (COMING SOON!) https://billingegroup.github.io/ml4ms.

Features
--------

Running the code with the database backend
------------------------------------------
* the code looks for a file called `ml4msrc.json` in the current directory for configuration information (`rc` stands for "run-control")
* this must be json format and contain a "client" item set to "fs" (this allows us later to switch to mongo)
* it must also define a database.  The database has
 * a "name" which can be whatever you want
 * a "url" and a "path".  For the file-system client, these are just concatenated to make a relative file-path to where
   database collection files are kept.  They are split out like this for when we have a remote mongo db in the cloud.
an example is below:

.. code-block:: JSON

  {
    "client": "fs",
    "databases": [
       {"name": "test_db",
        "url": ".",
        "path": "db",
        "public": false
       }
     ]
  }

Steps to use it this way:
=========================

1. install the package in your environment.  cd to ml4ms directory and run `pip install -e .` (you will have to do this again to update it)

2. to use it the first time
 1. create a sandbox directory where you want to play
 2. put a file called ml4msrc.json in it with a client and a database
 3. create a folder called `db` in that folder
 4. in the folder called `db` put all the collections you want in different files.

3. ok its all set up, now whenever you want to use it
 1. modify code in you the tinacode script (it will have a new name)
 2. open a terminal, navigate to the directory containing the `ml4msrc.json` file
 6. run `ml4ms` and it will run the code (you can do this from your IDE but you will have to set it up (`run` in PyCharm) to run in the sandbox directory
