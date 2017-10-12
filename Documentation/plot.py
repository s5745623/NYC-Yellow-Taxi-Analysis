import pygal




def plot_hours_count():
	hours_count = pygal.Line()
	hours_count.title = 'Hours_count of each week day'
	hours_count.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("result/hours_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wen', 'Tru', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = [0]
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[0] == i:
				if line_list[1] == '0-2':
					mydict[1] = line_list[2]
				elif line_list[1] == '2-4':
					mydict[2] = line_list[2]
				elif line_list[1] == '4-6':
					mydict[3] = line_list[2]
				elif line_list[1] == '6-8':
					mydict[4] = line_list[2]
				elif line_list[1] == '8-10':
					mydict[5] = line_list[2]
				elif line_list[1] == '10-12':
					mydict[6] = line_list[2]
				elif line_list[1] == '12-14':
					mydict[7] = line_list[2]
				elif line_list[1] == '14-16':
					mydict[8] = line_list[2]
				elif line_list[1] == '16-18':
					mydict[9] = line_list[2]
				elif line_list[1] == '18-20':
					mydict[10] = line_list[2]
				elif line_list[1] == '20-22':
					mydict[11] = line_list[2]
				elif line_list[1] == '22-24':
					mydict[12] = line_list[2]

		for key in sorted(mydict):
			temp.append(int(mydict[key]))

		hours_count.add(i, temp)

	hours_count.render()
	hours_count.render_to_file('hours_count.svg')


def plot_hours_sum():
	hours_sum = pygal.Line()
	hours_sum.title = 'Hours_sum of each week day'
	hours_sum.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("result/hours_sum.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wen', 'Tru', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = [0]
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[0] == i:
				if line_list[1] == '0-2':
					mydict[1] = line_list[2]
				elif line_list[1] == '2-4':
					mydict[2] = line_list[2]
				elif line_list[1] == '4-6':
					mydict[3] = line_list[2]
				elif line_list[1] == '6-8':
					mydict[4] = line_list[2]
				elif line_list[1] == '8-10':
					mydict[5] = line_list[2]
				elif line_list[1] == '10-12':
					mydict[6] = line_list[2]
				elif line_list[1] == '12-14':
					mydict[7] = line_list[2]
				elif line_list[1] == '14-16':
					mydict[8] = line_list[2]
				elif line_list[1] == '16-18':
					mydict[9] = line_list[2]
				elif line_list[1] == '18-20':
					mydict[10] = line_list[2]
				elif line_list[1] == '20-22':
					mydict[11] = line_list[2]
				elif line_list[1] == '22-24':
					mydict[12] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		hours_sum.add(i, temp)

	hours_sum.render()
	hours_sum.render_to_file('hours_sum.svg')


def plot_months_sum():
	months_sum = pygal.Line()
	months_sum.title = 'Months_sum of each week day'
	months_sum.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])
	month_dict={'1':'Jan','2':'Feb','3':'Mar','4':'Apr','5':'May','6':'Jun','7':'July','8':'Aug','9':'Sep','10':'Oct','11':'Nov','12':'Dec'}

	text_file = open("result/months_sum.txt", "r").readlines()

	months = ['1', '2', '3', '4', '5', '6', '7','8','9','10','11','12']
	weekdays = ['Mon', 'Tue', 'Wen', 'Tru', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = [0]
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		months_sum.add(i, temp)

	months_sum.render()
	months_sum.render_to_file('months_sum.svg')

def plot_months_count():
	months_count = pygal.Line()
	months_count.title = 'Months_count of each week day'
	months_count.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])

	text_file = open("result/months_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wen', 'Tru', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = [0]
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		months_count.add(i, temp)

	months_count.render()
	months_count.render_to_file('months_count.svg')


def plot_hours_distance_totalamount():
	hours_distance_totalamount = pygal.Line()
	hours_distance_totalamount.title = ''
	hours_distance_totalamount.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("result/hours_distance_totalamount.txt", "r").readlines()
	distance ={}
	totalamount ={}
	distance_list = []
	totalamount_list =[]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]

		if line_list[0] == '0-2':
			distance[1] = line_list[1]
			totalamount[1] = line_list[2]
		elif line_list[0] == '2-4':
			distance[2] = line_list[1]
			totalamount[2] = line_list[2]
		elif line_list[0] == '4-6':
			distance[3] = line_list[1]
			totalamount[3] = line_list[2]
		elif line_list[0] == '6-8':
			distance[4] = line_list[1]
			totalamount[4] = line_list[2]
		elif line_list[0] == '8-10':
			distance[5] = line_list[1]
			totalamount[5] = line_list[2]
		elif line_list[0] == '10-12':
			distance[6] = line_list[1]
			totalamount[6] = line_list[2]
		elif line_list[0] == '12-14':
			distance[7] = line_list[1]
			totalamount[7] = line_list[2]
		elif line_list[0] == '14-16':
			distance[8] = line_list[1]
			totalamount[8] = line_list[2]
		elif line_list[0] == '16-18':
			distance[9] = line_list[1]
			totalamount[9] = line_list[2]
		elif line_list[0] == '18-20':
			distance[10] = line_list[1]
			totalamount[10] = line_list[2]
		elif line_list[0] == '20-22':
			distance[11] = line_list[1]
			totalamount[11] = line_list[2]
		elif line_list[0] == '22-24':
			distance[12] = line_list[1]
			totalamount[12] = line_list[2]
	for key in sorted(distance):
		distance_list.append(float(distance[key]))
	for key in sorted(totalamount):
		totalamount_list.append(float(totalamount[key]))
	hours_distance_totalamount.add("The sum of distance", distance_list)
	hours_distance_totalamount.add("The sum of fare", totalamount_list, secondary=True)
	hours_distance_totalamount.render_to_file('hours_distance_totalamount.svg')



if __name__ == "__main__":
	plot_hours_count()
	plot_hours_sum()
	plot_months_sum()
	plot_months_count()
	plot_hours_distance_totalamount()