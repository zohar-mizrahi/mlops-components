import random
from parallelm.components.component import Component


class PiCalc(Component):
    def __init__(self):
        super(self.__class__, self).__init__()

    def materialize(self, sc, parents_rdds):
        if not parents_rdds:
            self._logger.error("Received an empty RDD input!")
            return

        rdd1 = parents_rdds[0]
        num_samples1 = rdd1.count()

        rdd2 = parents_rdds[1]
        num_samples2 = rdd2.count()

        total_samples = num_samples1 + num_samples2
        self._logger.info("Total num samples in RDDs: {}".format(total_samples))

        count1 = rdd1.filter(PiCalc._inside).count()
        count2 = rdd2.filter(PiCalc._inside).count()
        total_samples_inside = count1 + count2

        result = 4.0 * total_samples_inside / total_samples
        print("Pi is roughly {}".format(result))

        self._logger.info("Output model: {}".format(self._params['output-model']))
        with open(self._params['output-model'], "w") as f:
            f.write("Pi is: {}".format(result))

    @staticmethod
    def _inside(t):
        x, y = t[0], t[1]
        return x * x + y * y < 1
