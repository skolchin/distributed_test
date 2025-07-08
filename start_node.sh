#!/bin/bash
ray start --address=192.168.0.7:6379 --num-cpus=1 --resources='{"my_resource": 1}'