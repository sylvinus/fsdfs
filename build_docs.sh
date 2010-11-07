#!/bin/bash

FSDFSPATH=`pwd`
rm -f docs/_build/html
mkdir -p ../fsdfs-docs
cd ../fsdfs-docs
git clone git@github.com:sylvinus/fsdfs.git ./
git checkout gh-pages
git pull origin gh-pages
ln -s `pwd` ${FSDFSPATH}/docs/_build/html 
cd ${FSDFSPATH}
paver gh_pages_build
