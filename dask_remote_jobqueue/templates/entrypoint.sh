#!/bin/bash

source /cvmfs/cms.dodas.infn.it/miniconda3/bin/activate
conda activate af-test;

`echo $@ | sed -r "s/-c //"`
