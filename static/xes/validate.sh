#!/usr/bin/env bash

BAR=$(xmllint --noout --schema xes-ieee-1849-2016.xsd correct/*.xes)?
FOO=$?

# xmllint --noout --schema xes-ieee-1849-2016.xsd non_parsing/*.xes
# xmllint --noout --schema xes-ieee-1849-2016.xsd non_parsing/*.xes
# xmllint --noout --schema xes-ieee-1849-2016.xsd recoverable/*.xes

echo "foo <$FOO> $BAR"