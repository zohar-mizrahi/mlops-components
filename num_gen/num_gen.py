import numpy as np
from parallelm.components.component import Component


class NumGen(Component):
    num_samples = 0

    def __init__(self):
        super(self.__class__, self).__init__()

    def materialize(self, sc, parents_rdds):
        num_samples = self._params['num_samples']
        self._logger.info("Num samples: {}".format(num_samples))

        rdd = sc.parallelize([0] * num_samples).map(NumGen._rand_num)
        return [rdd]

    @staticmethod
    def _rand_num(x):
        return (np.random.random(), np.random.random())
# Some comment
