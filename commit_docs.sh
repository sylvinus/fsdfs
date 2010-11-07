#!/bin/bash

FSDFSPATH=`pwd`

./build_docs.sh
cd ../fsdfs-docs
git add *
git commit -a
git push origin gh-pages
cd ${FSDFSPATH}
