#!/usr/bin/env python
import sys
import random
import os
import math
import scipy.stats

from sandman2.api import operation, Session

import start

@operation
def agg(*outputs):
    # do nothing
   return outputs


def rake(hostname):
    replications = 4
    model = start.main
    m = operation(model, include_modules = True)
    # replications
    riskmodels = [1,2,3,4]
    jobs = []
    general_rc_event_schedule = []
    seeds = [random.randint(0, 2**32-1) for _ in range(replications)]
    print(seeds)

    for i in riskmodels:
        simulation_parameters['no_riskmodels'] = i

        if not general_rc_event_schedule:
            cat_separation_distribution = scipy.stats.expon(0, simulation_parameters["event_time_mean_separation"])

            for i in range(replications):
                rc_event_schedule = []
                for j in range(simulation_parameters["no_categories"]):
                    event_schedule = []
                    total = 0
                    while (total < simulation_parameters["max_time"]):
                        separation_time = cat_separation_distribution.rvs()
                        total += int(math.ceil(separation_time))
                        if total < simulation_parameters["max_time"]:
                            event_schedule.append(total)
                    rc_event_schedule.append(event_schedule)
                general_rc_event_schedule.append(rc_event_schedule)

            print(general_rc_event_schedule)

        job = [m(simulation_parameters,general_rc_event_schedule[x],seeds[x]) for x in range(replications)]
        print(len(general_rc_event_schedule))
        jobs.append(job)

    store = []

    nums = {'1': 'one', '2': 'two', '3': 'three', '4': 'four', '5': 'five',
            '6': 'six', '7': 'seven', '8': 'eight', '9': 'nine'}

    with Session(host=hostname, default_cb_to_stdout=True) as sess:
        counter = 1
        for job in jobs:
            result = sess.submit(job)
            
            wfile_0 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_cash.dat", "w")
            wfile_1 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_contracts.dat", "w")
            wfile_2 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_operational.dat", "w")
            wfile_3 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_reincash.dat", "w")
            wfile_4 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_reincontracts.dat", "w")
            wfile_5 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_reinoperational.dat", "w")
            wfile_6 = open(os.getcwd() + "/data/" + str(nums[str(counter)]) + "_premium.dat", "w")
            wfile_7 = open(os.getcwd() + "/data/" + str(counter) + "_rc_schedule.dat", "w")

            dirname = os.getcwd() + "/data/"
            number = str(nums[str(counter)])

            with open(dirname + number + "_cash.dat", "w") as f0, \
                 open(dirname + number + "_contracts.dat", "w") as f1, \
                 open(dirname + number + "_operational.dat", "w") as f2, \
                 open(dirname + number + "_reincash.dat", "w") as f3, \
                 open(dirname + number + "_reincontracts.dat", "w") as f4, \
                 open(dirname + number + "_reinoperational.dat", "w") as f5, \
                 open(dirname + number + "_premium.dat", "w") as f6, \
                 open(dirname + number + str(counter) + "_rc_schedule.dat", "w") as f7:

                for i in range(len(job)):
                    try:
                        os.stat(dirname)
                    except:
                        os.mkdir(dirname)

                    f0.write(str(result[i][0]) + "\n")
                    f1.write(str(result[i][1]) + "\n")
                    f2.write(str(result[i][2]) + "\n")
                    f3.write(str(result[i][3]) + "\n")
                    f4.write(str(result[i][4]) + "\n")
                    f5.write(str(result[i][5]) + "\n")
                    f6.write(str(result[i][6]) + "\n")
                    f7.write(str(result[i][7]) + "\n")

                counter =counter + 1
    print(store)


if __name__ == '__main__':
    host = None
    if len(sys.argv) > 1:
        host = sys.argv[1]
rake(host)
