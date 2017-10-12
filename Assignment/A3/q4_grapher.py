#!/usr/bin/python34
#
# q4 graph
#
import matplotlib.pyplot as plt

if __name__=="__main__":

    plot_hour = []
    plot_stdev = [] 

    with open("q4_hop23.txt", 'r') as f:
        lines = f.readlines()

        for line in lines: 
            hour, stdev = line.split("\t")

            plot_hour.append(hour)
            plot_stdev.append(stdev)


        plt.plot(plot_hour,plot_stdev, "r^-",)
        plt.title("q4_graph")
     
        plt.ylabel("standard deviation")
        plt.xlabel("Hour")
        # This would display locally:
        # plt.show()
        # This will save the figure as a png and as a PDF:
        plt.savefig("q4_graph.png")
        plt.savefig("q4_graph.pdf")