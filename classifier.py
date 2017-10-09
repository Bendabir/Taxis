from cassandra.cluster import Cluster
import sys
from coordonates import Coordonate
import sklearn.cluster as clst
import urllib.request
import urllib.parse
import math
import random

# Return the euclidian distance bewteen 2 points of dimension n
def dist(x, y):
    if len(x) != len(y):
        raise Exception('Dimensions are different : x (%d) and y (%d).' % (len(x), len(y)))

    return math.sqrt(sum([(x[i] - y[i]) ** 2 for i in range(len(x))]))



class Classifier:
    def __init__(self):
        self.cluster = None
        self.session = None

        self.startCoordonates = []
        self.arrivalCoordonates = []

        try:
            self.cluster = Cluster(['127.0.0.1'])
            self.session = self.cluster.connect('e21')
            self.session.default_timeout = 9999
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            sys.exit(1)


    def loadStartCoordonates(self):
        try:
            print('Loading start coordonates...')

            results = self.session.execute(
                '''
                SELECT start_zone_lon, start_zone_lat
                FROM e21.facts_by_start_zone;
                -- LIMIT 100000;
                '''
            )

            self.startCoordonates = []

            for r in results:
                self.startCoordonates.append([r.start_zone_lon, r.start_zone_lat])

            print('Done loading start coordonates.')
        except Exception as e:
            print('Error getting start coordonates : %s' % (str(e)))
            return


    def loadArrivalCoordonates(self):
        try:
            print('Loading arrival coordonates...')

            results = self.session.execute(
                '''
                SELECT arrival_zone_lon, arrival_zone_lat
                FROM e21.facts_by_arrival_zone;
                -- LIMIT 100000;
                '''
            )

            self.arrivalCoordonates = []

            for r in results:
                self.arrivalCoordonates.append([r.arrival_zone_lon, r.arrival_zone_lat])

            print('Done loading arrival coordonates.')
        except Exception as e:
            print('Error getting arrival coordonates : %s' % (str(e)))
            return                    



    # Implementing kmeans (online)
    def kmeans(self, k, cutoff = 1e-3, max_iterations = 100):
        print('Start classifying...')

        n = 4 # Dimension of the set
        vn = [0 for i in range(n)] # Default centroid (0)

        centroids = [[0 for j in range(n)] for i in range(k)]
        
        # Centroids, random
        for i in range(0, k):
            for j in range(0, n, 2):
                centroids[i][j] = random.uniform(-8.8, -8.5) # Lon
                centroids[i][j + 1] = random.uniform(41, 41.3) # Lat

        it = 0 # Number of iterations

        # Looping until convergence
        while it < max_iterations:
            parameters = [[0, vn] for i in range(k)]

            # Requesting data
            results = self.session.execute(
                '''
                SELECT start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat
                FROM e21.facts_by_start_zone;
                '''
            )

            # While we get data
            for r in results:
                x = [r.start_zone_lon, r.start_zone_lat, r.arrival_zone_lon, r.arrival_zone_lat]

                # Need to check if we have some 0
                if 0 in x:
                    continue

                # Compute distance
                distances = [dist(x, c) for c in centroids]

                minIndex = int(distances.index(min(distances))) # which.min, closest centroid from x

                p = parameters[minIndex]

                p[0] += 1
                p[1] = [p[1][j] + x[j] for j in range(len(x))]

                parameters[minIndex] = p

            it += 1

            # If no convergence after max_iterations
            if it == max_iterations:
                print('No convergence after %d iterations...' % (max_iterations))
                return []

            oldCentroids = list(centroids)

            for i in range(k):
                # Avoiding a fucking weird bug where the list exists twice...
                if parameters[i][0] != 0:
                    centroids[i] = [float(e) / parameters[i][0] for e in parameters[i][1]]

            biggestShift = 0.0

            # Checking if it has moved
            for i in range(k):
                shift = dist(centroids[i], oldCentroids[i])

                biggestShift = max(biggestShift, shift)

            print('Iteration %d : %.4f' % (it, biggestShift))

            # If converged
            if biggestShift < cutoff:
                break

        print('Done classifying (after %d iterations).' % (it))

        return centroids


    def getTripsMap(self, k = 5):
        centroids = self.kmeans(k)

        if len(centroids) == 0:
            return

        # Splitting and reversing
        startCenters = [c[:2][::-1] for c in centroids]
        arrivalCenters = [c[-2:][::-1] for c in centroids]

        paths = ''

        # Building paths
        for i in range(k):
            paths += '&path=' + ','.join(map(str, startCenters[i])) + '%7C' + ','.join(map(str, arrivalCenters[i]))

        print('Getting map image from Google...')

        # Representing points on a map
        googleMapsApiKey = 'AIzaSyBEts-SoJGGghbBmkFO_PbgI24104vGrhI'

        # coord[::-1] reverse the list
        startMarkers = '%7C'.join([','.join(map(str, coord)) for coord in startCenters])
        arrivalMarkers = '%7C'.join([','.join(map(str, coord)) for coord in arrivalCenters])

        googleMapsURL = 'https://maps.googleapis.com/maps/api/staticmap?&size=2048x2048&markers=color:blue%7Clabel:D%7C' + startMarkers + '&markers=color:red%7Clabel:A%7C' + arrivalMarkers + paths + '&key=' + googleMapsApiKey

        # Saving the image
        try:
            urllib.request.urlretrieve(googleMapsURL, filename = 'kmeans-' + str(k) + '-map-trips.png')
        except Exception as e:
            print('Error getting image : %e' % (str(e)))
            print('URL was : %s' % googleMapsURL)
            return 

        print('Image saved !')



    def classify(self, k, coordonatesArray):
        if len(coordonatesArray) == 0:
            return []

        print('Running classifier...')

        # Filtering data
        coordonatesArray = [[x, y] for [x, y] in coordonatesArray if x != 0 and y != 0]

        kmeans = clst.KMeans(n_clusters = k).fit(coordonatesArray)

        roundNumber = 3 # Number of decimals

        centers = [[float(x), float(y)] for [x, y] in kmeans.cluster_centers_]
        centers = [[round(x, roundNumber), round(y, roundNumber)] for [x, y] in centers]

        print('Done classifying.')

        return {
            'centers': centers,
            'inertia': kmeans.inertia_
        }


    def getMaps(self, k = 5):
        # Load data
        self.loadStartCoordonates()
        self.loadArrivalCoordonates()

        startKmeans = self.classify(k, self.startCoordonates)
        arrivalKmeans = self.classify(k, self.arrivalCoordonates)

        print('Getting map image from Google...')

        # Representing points on a map
        googleMapsApiKey = 'AIzaSyBEts-SoJGGghbBmkFO_PbgI24104vGrhI'

        # coord[::-1] reverse the list
        startMarkers = '%7C'.join([','.join(map(str, coord[::-1])) for coord in startKmeans['centers']])
        arrivalMarkers = '%7C'.join([','.join(map(str, coord[::-1])) for coord in arrivalKmeans['centers']])

        googleMapsURL = 'https://maps.googleapis.com/maps/api/staticmap?&size=2048x2048&markers=color:blue%7Clabel:D%7C' + startMarkers + '&markers=color:red%7Clabel:A%7C' + arrivalMarkers + '&key=' + googleMapsApiKey

        # Saving the image
        try:
            urllib.request.urlretrieve(googleMapsURL, filename = 'kmeans-' + k + '-map.png')
        except Exception as e:
            print('Error getting image : %e' % (str(e)))
            print('URL was : %s' % googleMapsURL)
            return 

        print('Image saved !')


# If executing file, then run the script
if __name__ == '__main__':
    clf = Classifier()