#!/usr/bin/env python3
from time import time
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.xes import factory as xes_exporter

in_path = '../static/xes/book/bigger-example.xes'
out_path = '/tmp/out.xes'


print(f'{in_path} --> {out_path}')

t_s = time()
log = xes_import_factory.apply(in_path)
t_m = time()
xes_exporter.export_log(log, out_path)
t_e = time()

print(f'read in {t_m-t_s:.3f}s, wrote in {t_e-t_m:.3f}s')
