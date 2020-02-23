def getkeys(DEVELOPER_KEY, MAIN_KEYS):
	keysfile = open("keysamen.txt")
	keys = keysfile.readlines()
	line_number = 1
	for key in keys:
		if line_number == 1:
			DEVELOPER_KEY.append(key.strip())
		else:
			MAIN_KEYS.append(key.strip())
		line_number += 1




