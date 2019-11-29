import sys

inline = sys.stdin


prev_line = ''

for line in inline:
	line = line.strip()
	if(prev_line!=line):
		if(prev_line!=''):
			print(prev_line)
		prev_line = line
		
print(prev_line)