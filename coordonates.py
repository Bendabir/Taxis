import math

class Coordonate:
    earthRadius = 6371008 # In meters

    def __init__(self, longitude, latitude):
        self.lon = longitude
        self.lat = latitude


    def __str__(self):
        return '(%f, %f)' % (self.lon, self.lat)


    def distanceFrom(self, coord):
        return math.sqrt(((self.lat - coord.lat) / 360 * 2 * math.pi * self.earthRadius)**2 + ((self.lon - coord.lon) / 360 * 2 * math.pi * self.earthRadius * math.cos((self.lat + coord.lat) / 360 * math.pi))**2)        
