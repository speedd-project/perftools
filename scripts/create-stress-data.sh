#!/bin/sh
./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x1.csv -r 1

./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x2.csv -r 2

./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x10.csv -r 10

./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x50.csv -r 50

./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x100.csv -r 100

./genevents -s ../../speedd/speedd-runtime/scripts/ccf/FeedzaiIntegrationData.csv -t../../speedd/speedd-runtime/scripts/ccf/stress-data-x1000.csv -r 1000
