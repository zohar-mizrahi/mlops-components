import random
from parallelm.components.component import Component

# from parallelm.mlstats import mlstats as pm
# from parallelm.mlstats import StatCategory as st
# from parallelm.mlstats.stats.multi_line_graph import MultiLineGraph
# from parallelm.mlstats.stats.mlstat_table import Table


class PiCalc(Component):
    def __init__(self):
        super(self.__class__, self).__init__()

    def materialize(self, sc, parents_rdds):
        if not parents_rdds:
            self._logger.error("Received an empty RDD input!")
            return

        # pm.init(sc)
        #
        # pm.stat("stat1", 1.0, st.TIME_SERIES)
        # pm.stat("stat1", 2.0, st.TIME_SERIES)
        # pm.stat("stat1", 3.0, st.TIME_SERIES)
        # pm.stat("stat1", 4.0, st.TIME_SERIES)
        # pm.stat("stat1", 5.0, st.TIME_SERIES)
        # pm.stat("stat1", 6.0, st.TIME_SERIES)
        # pm.stat("stat1", 7.0, st.TIME_SERIES)
        # pm.stat("stat1", 8.0, st.TIME_SERIES)

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

