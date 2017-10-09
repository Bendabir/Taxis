centroids = [
	[1, 2, 3, 4],
	[5, 6, 7, 8]
]

startCenters = [c[:2][::-1] for c in centroids]
arrivalCenters = [c[-2:][::-1] for c in centroids]

print startCenters
print arrivalCenters

paths = ''

# Building paths
for i in range(len(centroids)):
    paths += '&path=' + ','.join(map(str, startCenters[i])) + '|' + ','.join(map(str, arrivalCenters[i]))

print paths