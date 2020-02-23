# import csv
#
# keyFile = open("keysfile.csv", 'r')
# reader = csv.reader(keyFile)
# Main_Key = []
# Developer_Key = []
#
# for key in reader:
# 	if reader.line_num == 1:
# 		Developer_Key.append(key)
# 	else:
# 		Main_Key.append(key)

def getkeys(DEVELOPER_KEY, MAIN_KEYS):
	keysfile = open("keys.txt")
	keys = keysfile.readlines()
	line_number = 1
	for key in keys:
		if line_number == 1:
			DEVELOPER_KEY.append(key.strip())
		else:
			MAIN_KEYS.append(key.strip())
		line_number += 1




