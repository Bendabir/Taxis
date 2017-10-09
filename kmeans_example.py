# Une itération
# Vn vecteur nul de dimension adéquate (n = 4 dans notre cas)
sommes = [[0, vn], [0, vn], [0, vn]]

for xi in :
	d = [dist(xi - c) for c in centroids]
	K = wich.min(d)

	sommes[K][0] += 1 
	sommes[K][1] += xi

centroids = [s[1] / s[0] for s in sommes]