#!/bin/sh
aws s3 sync ~/dcp_helper/metadata s3://ascstore/dcp_helper/metadata --exclude ".git*"
