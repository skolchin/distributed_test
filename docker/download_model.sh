#!/bin/bash

if ! hf cache scan | grep "${1}" > /dev/null; then
    echo "Model ${1} was NOT found in cache, trying to download"

    if [[ -z "${HUGGINGFACE_API_KEY}" ]]; then
        echo "Warning: HuggingFace API key is not provided, new model download might fail"
    else
        echo "Logging in to HuggingFace"
        hf auth login --token "${HUGGINGFACE_API_KEY}" > /dev/null || (echo "Error while logging in, check your API key is correct"  && exit 1)
    fi

    echo "Downloading model ${1}"
    hf download $1 || (echo "Error while downloading model"  && exit 1)
    echo "Download successfull"
fi
