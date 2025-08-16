with open("tmp.bin", "r+b") as f:
	f.seek(10)
	buffer = b'\xff\xff\xff\xff'
	f.write(buffer)

with open("tmp.bin", "rb") as f:
	print(f.read())

with open("tmp.bin", "r+b") as f:
	f.seek(1)
	buffer = b'\xff\xff\xff\xff'
	f.write(buffer)

with open("tmp.bin", "rb") as f:
	print(f.read())
