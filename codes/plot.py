import pygal



# fare
def plot_hours_count():
	hours_count = pygal.Line()
	hours_count.title = 'Fare count of each two hours in each week day '
	hours_count.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("compute_result/hours_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

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
	hours_count.render_to_png('hours_count.png') 


def plot_hours_sum():
	hours_sum = pygal.Line()
	hours_sum.title = 'Fare sum of each two hours in each week day'
	hours_sum.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("compute_result/hours_sum.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

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
	hours_sum.render_to_png('hours_sum.png') 


def plot_months_sum():
	months_sum = pygal.Line()
	months_sum.title = 'Fare sum of each week day in each month'
	months_sum.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])
	month_dict={'1':'Jan','2':'Feb','3':'Mar','4':'Apr','5':'May','6':'Jun','7':'July','8':'Aug','9':'Sep','10':'Oct','11':'Nov','12':'Dec'}

	text_file = open("compute_result/months_sum.txt", "r").readlines()

	months = ['1', '2', '3', '4', '5', '6', '7','8','9','10','11','12']
	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = []
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		months_sum.add(i, temp)

	months_sum.render()
	months_sum.render_to_file('months_sum.svg')
	months_sum.render_to_png('months_sum.png') 

def plot_months_count():
	months_count = pygal.Line()
	months_count.title = 'Fare count of each week day in each month'
	months_count.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])

	text_file = open("compute_result/months_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = []
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		months_count.add(i, temp)

	months_count.render()
	months_count.render_to_file('months_count.svg')
	months_count.render_to_png('months_count.png') 


def plot_hours_distance_totalamount():
	hours_distance_totalamount = pygal.Line()
	hours_distance_totalamount.title = 'Distance and total amount of each two hours'
	hours_distance_totalamount.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("compute_result/hours_distance_totalamount.txt", "r").readlines()
	distance ={}
	totalamount ={}
	distance_list = [0]
	totalamount_list =[0]
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
	hours_distance_totalamount.add("The sum of distances", distance_list)
	hours_distance_totalamount.add("The sum of total amounts", totalamount_list, secondary=True)
	hours_distance_totalamount.render_to_file('hours_distance_totalamount.svg')
	hours_distance_totalamount.render_to_png('hours_distance_totalamount.png') 

	
# weekdays

def plot_weekdays_amount():
	weekdays_amount = pygal.Line()
	weekdays_amount.title = 'Weekdays amount'
	weekdays_amount.x_labels = map(str, ['Monday', 'Tuesday','Wednesday', 'Thursday', 'Friday','Saturday','Sunday'])

	text_file1 = open("result/tips_weekdays_amount.txt", "r").readlines()
	text_file2 = open("result/fare_weekdays_amount.txt", "r").readlines()

	fare_dict = {}
	tips_dict = {}
	fare_list =[]
	tips_list = []

	for line in text_file1:
		line_list = [x.strip() for x in line.split('\t')]
		if line_list[0] =='Mon' :
			tips_dict[1] = line_list[1]
		elif  line_list[0] =='Tue' :
			tips_dict[2] = line_list[1]
		elif line_list[0] == 'Wed' :
			tips_dict[3] = line_list[1]
		elif line_list[0] =='Thu' :
			tips_dict[4] = line_list[1]
		elif line_list[0] == 'Fri':
			tips_dict[5] = line_list[1]
		elif line_list[0] == 'Sat':
			tips_dict[6] = line_list[1]
		elif line_list[0] == 'Sun' :
			tips_dict[7] = line_list[1]

	for key in sorted(tips_dict):
		tips_list.append(float(tips_dict[key]))

	for line in text_file2:
		line_list = [x.strip() for x in line.split('\t')]
		if line_list[0] =='Mon' :
			fare_dict[1] = line_list[1]
		elif  line_list[0] =='Tue' :
			fare_dict[2] = line_list[1]
		elif line_list[0] == 'Wed' :
			fare_dict[3] = line_list[1]
		elif line_list[0] =='Thu' :
			fare_dict[4] = line_list[1]
		elif line_list[0] == 'Fri':
			fare_dict[5] = line_list[1]
		elif line_list[0] == 'Sat':
			fare_dict[6] = line_list[1]
		elif line_list[0] == 'Sun' :
			fare_dict[7] = line_list[1]

	for key in sorted(fare_dict):
		fare_list.append(float(fare_dict[key]))


	weekdays_amount.add('Fare amount', fare_list)
	weekdays_amount.add('Tips amount', tips_list)
	weekdays_amount.render_to_file('weekdays_amount.svg')
	weekdays_amount.render_to_png('weekdays_amount.png')



def plot_weekdays_sum():
	weekdays_sum = pygal.Line()
	weekdays_sum.title = 'Weekdays count'
	weekdays_sum.x_labels = map(str, ['Monday', 'Tuesday','Wednesday', 'Thursday', 'Friday','Saturday','Sunday'])

	text_file1 = open("result/tips_weekdays_sum.txt", "r").readlines()
	text_file2 = open("result/fare_weekdays_sum.txt", "r").readlines()

	fare_dict = {}
	tips_dict = {}
	fare_list =[]
	tips_list = []

	for line in text_file1:
		line_list = [x.strip() for x in line.split('\t')]
		if line_list[0] =='Mon' :
			tips_dict[1] = line_list[1]
		elif  line_list[0] =='Tue' :
			tips_dict[2] = line_list[1]
		elif line_list[0] == 'Wed' :
			tips_dict[3] = line_list[1]
		elif line_list[0] =='Thu' :
			tips_dict[4] = line_list[1]
		elif line_list[0] == 'Fri':
			tips_dict[5] = line_list[1]
		elif line_list[0] == 'Sat':
			tips_dict[6] = line_list[1]
		elif line_list[0] == 'Sun' :
			tips_dict[7] = line_list[1]

	for key in sorted(tips_dict):
		tips_list.append(float(tips_dict[key]))

	for line in text_file2:
		line_list = [x.strip() for x in line.split('\t')]
		if line_list[0] =='Mon' :
			fare_dict[1] = line_list[1]
		elif  line_list[0] =='Tue' :
			fare_dict[2] = line_list[1]
		elif line_list[0] == 'Wed' :
			fare_dict[3] = line_list[1]
		elif line_list[0] =='Thu' :
			fare_dict[4] = line_list[1]
		elif line_list[0] == 'Fri':
			fare_dict[5] = line_list[1]
		elif line_list[0] == 'Sat':
			fare_dict[6] = line_list[1]
		elif line_list[0] == 'Sun' :
			fare_dict[7] = line_list[1]

	for key in sorted(fare_dict):
		fare_list.append(float(fare_dict[key]))


	weekdays_sum.add('Fare count', fare_list)
	weekdays_sum.add('Tips count', tips_list)
	weekdays_sum.render_to_file('weekdays_count.svg')
	weekdays_sum.render_to_png('weekdays_count.png')


# tips

def plot_tips_hours_count():
	tips_hours_count = pygal.Line()
	tips_hours_count.title = 'Tips count of each week day hours'
	tips_hours_count.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("result/tips_hours_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

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

		tips_hours_count.add(i, temp)

	tips_hours_count.render()
	tips_hours_count.render_to_file('tips_hours_count.svg')
	tips_hours_count.render_to_png('tips_hours_count.png')


def plot_tips_hours_sum():
	tips_hours_sum = pygal.Line()
	tips_hours_sum.title = 'Tips sum of each week day hours'
	tips_hours_sum.x_labels = map(str, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24])

	text_file = open("result/tips_hours_sum.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

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

		tips_hours_sum.add(i, temp)

	tips_hours_sum.render()
	tips_hours_sum.render_to_file('tips_hours_sum.svg')
	tips_hours_sum.render_to_png('tips_hours_sum.png')


def plot_tips_months_sum():
	tips_months_sum = pygal.Line()
	tips_months_sum.title = 'Tips sum of each week day in each month'
	tips_months_sum.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])
	month_dict={'1':'Jan','2':'Feb','3':'Mar','4':'Apr','5':'May','6':'Jun','7':'July','8':'Aug','9':'Sep','10':'Oct','11':'Nov','12':'Dec'}

	text_file = open("result/tips_months_sum.txt", "r").readlines()

	months = ['1', '2', '3', '4', '5', '6', '7','8','9','10','11','12']
	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = []
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		tips_months_sum.add(i, temp)

	tips_months_sum.render()
	tips_months_sum.render_to_file('tips_months_sum.svg')
	tips_months_sum.render_to_png('tips_months_sum.png')

def plot_tips_months_count():
	tips_months_count = pygal.Line()
	tips_months_count.title = 'Tips count of each week day in each month'
	tips_months_count.x_labels = map(str, ['Jan','Feb','Mar','Apr','May','Jun','July','Aug','Sep','Oct','Nov','Dec'])

	text_file = open("result/tips_months_count.txt", "r").readlines()

	weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

	for i in weekdays:
		mydict = {}
		temp = []
		for line in text_file:
			line_list = [x.strip() for x in line.split('\t')]
			if line_list[1] == i:
				mydict[int(line_list[0])] = line_list[2]

		for key in sorted(mydict):
			temp.append(float(mydict[key]))

		tips_months_count.add(i, temp)

	tips_months_count.render()
	tips_months_count.render_to_file('tips_months_count.svg')
	tips_months_count.render_to_png('tips_months_count.png')


def plot_area_0_4():
	area_0_4 = pygal.HorizontalBar()
	area_0_4.title = 'The most frequent pickup zone on Friday from 0 AM to 4 AM '
	text_file = open("result/area_0_4.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_0_4.add(line_list[0],int(line_list[1]))
	area_0_4.render()
	# area_0_4.render_to_png("area_0_4.png")
	area_0_4.render_to_file('area_0_4.svg')


def plot_area_4_8():
	area_4_8 = pygal.HorizontalBar()
	area_4_8.title = 'The most frequent pickup zone on Friday from 4 AM to 8 AM '
	text_file = open("result/area_4_8.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_4_8.add(line_list[0],int(line_list[1]))
	area_4_8.render()
	area_4_8.render_to_file("area_4_8.svg")

def plot_area_8_12():
	area_8_12 = pygal.HorizontalBar()
	area_8_12.title = 'The most frequent pickup zone on Friday from 8 AM to 12 AM '
	text_file = open("result/area_8_12.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_8_12.add(line_list[0],int(line_list[1]))
	area_8_12.render()
	area_8_12.render_to_file("area_8_12.svg")

def plot_area_12_16():
	area_12_16 = pygal.HorizontalBar()
	area_12_16.title = 'The most frequent pickup zone on Friday from 12 PM to 16 PM '
	text_file = open("result/area_12_16.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_12_16.add(line_list[0],int(line_list[1]))
	area_12_16.render()
	area_12_16.render_to_file("area_12_16.svg")

def plot_area_16_20():
	area_16_20 = pygal.HorizontalBar()
	area_16_20.title = 'The most frequent pickup zone on Friday from 16 PM to 20 PM '
	text_file = open("result/area_16_20.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_16_20.add(line_list[0],int(line_list[1]))
	area_16_20.render()
	area_16_20.render_to_file("area_16_20.svg")

def plot_area_20_24():
	area_20_24 = pygal.HorizontalBar()
	area_20_24.title = 'The most frequent pickup zone on Friday from 20 PM to 24 PM '
	text_file = open("result/area_20_24.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_20_24.add(line_list[0],int(line_list[1]))
	area_20_24.render()
	area_20_24.render_to_file("area_20_24.svg")

def plot_area_fri():
	area_20_24 = pygal.HorizontalBar()
	area_20_24.title = 'The most frequent pickup zone on Friday'
	text_file = open("result/area_fri.txt","r").readlines()[0:5]
	for line in text_file:
		line_list = [x.strip() for x in line.split('\t')]
		area_20_24.add(line_list[0],int(line_list[1]))
	area_20_24.render()
	area_20_24.render_to_file("area_fri.svg")




if __name__ == "__main__":
	plot_hours_count()
	plot_hours_sum()
	plot_months_sum()
	plot_months_count()
	plot_hours_distance_totalamount()
	plot_weekdays_amount()
	plot_weekdays_sum()
	plot_tips_hours_count()
	plot_tips_hours_sum()
	plot_tips_months_sum()
	plot_tips_months_count()
	plot_area_0_4()
	plot_area_4_8()
	plot_area_8_12()
	plot_area_12_16()
	plot_area_16_20()
	plot_area_20_24()
	plot_area_fri()
