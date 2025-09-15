#!/bin/bash

if [[ -z $(hf cache scan | grep "${1}") ]]; then
    echo "Model ${1} was not found in cache"

    if [[ -z "${2}" ]]; then
        echo "Warning: HuggingFace API key is not provided, new model download might fail"
    else
        echo "Logging in to HuggingFace"
        hf auth login --token $2 > /dev/null || (echo "Error while logging in, check your API key is correct"  && exit 1)
        export HUGGINGFACE_API_KEY=$2
    fi

    echo "Downloading model ${1}"
    hf download $1 || (echo "Error while downloading model"  && exit 1)
    echo "Download successfull"
fi
