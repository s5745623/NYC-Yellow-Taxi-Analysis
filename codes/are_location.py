from geopy.geocoders import Nominatim
import re

def area_0_4_location():

	text_file = open("result/area_0_4.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_0_4_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			print (location)
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')
			

def area_4_8_location():

	text_file = open("result/area_4_8.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_4_8_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')

def area_8_12_location():

	text_file = open("result/area_8_12.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_8_12_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')

def area_12_16_location():

	text_file = open("result/area_12_16.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_12_16_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')

def area_16_20_location():

	text_file = open("result/area_16_20.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_16_20_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')

def area_20_24_location():

	text_file = open("result/area_20_24.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_20_24_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')

def area_fri_location():

	text_file = open("result/area_fri.txt","r").readlines()[0:5]
	geolocator = Nominatim()
	with open ("area_fri_location.txt","w") as f:
		for line in text_file:
			# line_list = [x.strip() for x in line.split('\t')]
			line_list = re.split('/|\t', line)
			location = geolocator.geocode(line_list[0])
			f.write(str(location.latitude)+'\t'+str(location.longitude)+'\n')


if __name__ == "__main__":

	area_0_4_location()
	area_4_8_location()
	area_8_12_location()
	area_12_16_location()
	area_16_20_location()
	area_20_24_location()
	area_fri_location()





