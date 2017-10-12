#!/usr/bin/python34
#
# q4 graph
#
import matplotlib.pyplot as plt

if __name__=="__main__":

    plot_hour = []
    plot_stdev = [] 
    explode = (0,0,0,0,0,0,0,0,0,0,0,0.3,0,0,0,0,0,0,0,0,0,0,0,0)
    colors = ["#E13F29", "#D69A80", "#D63B59", "#AE5552", "#CB5C3B", "#EB8076", "#96624E"]
    with open("q4_hop23.txt", 'r') as f:
        lines = f.readlines()

        for line in lines: 
            hour, stdev = line.split("\t")

            plot_hour.append(hour)
            plot_stdev.append(stdev)


        fig1, ax1 = plt.subplots()
        ax1.pie(plot_stdev,explode=explode, labels=plot_hour, autopct='%.1f',pctdistance=0.9,
                shadow=True,colors=colors,startangle=90)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

      
        # This would display locally:
        # plt.show()
        # This will save the figure as a png and as a PDF:
        plt.savefig("q4_graph_percentage.png")
        plt.savefig("q4_graph_percentage.pdf")