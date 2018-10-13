'''
Created on Oct 09, 2017

@author: Francesco Marconi
'''
import factory_cfg

class BenchInstanceFactory(object):
    '''
    classdocs
    '''
    __supported_providers = factory_cfg.supported_providers


    @staticmethod
    def get_bench_instance(provider, cluster_id):
        bench_instance = BenchInstanceFactory.__supported_providers.get(provider, None)
        if bench_instance:
            return bench_instance(cluster_id=cluster_id)
        else:
            raise NotImplementedError("The requested provider ({}) is not supported.".format(provider))
