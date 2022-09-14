#!/bin/bash

while getopts ":hdl" option; do
   case $option in
      h) # display Help
        echo "Script to start the server on your machine"
        echo
        echo "Syntax: ./local_server.sh [-h|d|l]"
        echo "options:"
        echo "h     Print this Help."
        echo "d     Run the server in dev tier ( all resources are provisioned ). Optionally specify the tier id ( default is 106 )."
        echo "l     Run the server in local test tier ( all resource are created and destroyed, does not have access to AWS services that need seperate provisioning)."
        exit;;
      d) # Run dev tier
        echo "Running dev tier"
        if [ -z "$2" ]; then
          tier_id=106
        else
          tier_id=$2
        fi
        cp devenv.rc devenv_temp.rc
        sed -i "s/-106-/-$tier_id-/g" devenv_temp.rc
        sed -i "s/=106$/=$tier_id/g" devenv_temp.rc
        source devenv_temp.rc
        python e2etests/run_local.py dev $tier_id
        source testenv.rc
        rm devenv_temp.rc
        exit;;
      l) # Run local test tier
        echo "Running local test tier"
        source testenv.rc
        python e2etests/run_local.py local_test
        exit;;
      \?) # Invalid option
         echo "Error: Invalid option, supported options -d (dev) and -l (local)"
         exit;;
   esac
done
echo "Error: No option specified, supported options -d (dev) and -l (local)"
