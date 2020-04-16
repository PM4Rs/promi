#!/usr/bin/env bash

xmllint --noout --schema xes-ieee-1849-2016.xsd correct/*.xes
# xmllint --noout --schema xes-ieee-1849-2016.xsd non_parsing/*.xes
# xmllint --noout --schema xes-ieee-1849-2016.xsd non_parsing/*.xes
# xmllint --noout --schema xes-ieee-1849-2016.xsd recoverable/*.xes
