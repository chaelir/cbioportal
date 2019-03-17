#!/bin/bash
export PYTHONPATH=${PYTHONPATH}:${HOME}/setup/cbioportal-docker/cbioportal/core/target/scripts
python3 system_tests_validate_studies.py
python3 system_tests_validate_data.py
python3 unit_tests_validate_data.py
