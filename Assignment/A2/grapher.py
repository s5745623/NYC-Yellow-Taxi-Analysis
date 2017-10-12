#!/usr/lib/env python35

# for more information about matplotlib, see:
# http://matplotlib.org/users/pyplot_tutorial.html

import numpy as np
import matplotlib.pyplot as plt

if __name__=="__main__":
    # Create some fake data
    run1 = [1,2,3]
    run2 = [2,4,5]
    run3 = [1,10,2]

    xpoints = [1,2,3]

    run1_line = plt.plot(xpoints,run1, "r--", label="run1")
    run2_line = plt.plot(xpoints,run2, "bs-", label="run2")
    run3_line = plt.plot(xpoints,run3, "g^-", label="run3")
    plt.title("A comparision of three runs")
    plt.legend()

    # This would display locally:
    # plt.show()
    # This will save the figure as a png and as a PDF:
    plt.savefig("graph_output.png")
    plt.savefig("graph_output.pdf")
