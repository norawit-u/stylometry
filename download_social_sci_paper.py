import subprocess
# wget http://file.scirp.org/xml/{52109..78000}.xml

start = 52109
end = 78000
worker = 10
# subprocess.call(['wget http://file.scirp.org/xml/{52109..54697}.xml'],shell=True)
step = (end-start)/worker
for i in range(start,int(78000-step),int(step)):
	print('wget -m http://file.scirp.org/xml/{'+str(i)+'..'+(str(int(i)+int(step)-1))+'}.xml')
	# subprocess.Popen(['wget -nc http://file.scirp.org/xml/{'+str(i)+'..'+(str(int(i)+int(step)))+'}.xml'],shell=True)