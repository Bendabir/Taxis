import csv
from cassandra.cluster import Cluster
import math
import datetime
import time
# import ast
import json


class Coordonate:
    earthRadius = 6371008 # In meters

    def __init__(self, longitude, latitude):
        self.lon = longitude
        self.lat = latitude


    def __str__(self):
        return '(%f, %f)' % (self.lon, self.lat)


    def distanceFrom(self, coord):
        return math.sqrt(((self.lat - coord.lat) / 360 * 2 * math.pi * self.earthRadius)**2 + ((self.lon - coord.lon) / 360 * 2 * math.pi * self.earthRadius * math.cos((self.lat + coord.lat) / 360 * math.pi))**2)        




class Cleaner:
    """Data cleaner for taxis data"""
    publicHolidays = [
        # -- 2013 --

        '01/01/2013',
        # '11/02/2013', # Regional holiday
        # '14/02/2013', # Ordinary Thursday
        '29/03/2013',
        '31/03/2013',
        # '01/04/2013', # Regional holiday
        '25/04/2013',
        '01/05/2013',
        '05/05/2013', # Ordinary Sunday
        # '09/05/2013', # Regional holiday
        # '30/05/2013', # Ordinary Thursday
        '10/06/2013',
        # '13/06/2013', # Regional holiday
        # '24/06/2013', # Regional holiday
        # '29/06/2013', # Regional holiday
        '15/08/2013',
        # '05/10/2013', # Ordinary Saturday
        # '01/11/2013', # Ordinary Friday
        '01/12/2013', # Ordinary Sunday
        '08/12/2013',
        '25/12/2013',


        # -- 2014 --
        
        '01/01/2014',
        # '14/02/2014', # Ordinary Friday
        '03/03/2014', # Regional holiday
        '18/04/2014',
        '20/04/2014',
        # '21/04/2014', # Regional holiday
        '25/04/2014', 
        '01/05/2014', 
        '04/05/2014', # Ordinary Sunday
        # '29/05/2014', # Regional holiday
        '10/06/2014',
        # '13/06/2014', # Regional holiday
        # '19/06/2014', # Ordinay Thursday
        # '24/06/2014', # Regional holiday
        '29/06/2014', # Regional holiday
        '15/08/2014',
        '05/10/2014', # Ordinary Sunday
        # '01/11/2014', # Ordinary Saturday
        '01/12/2014', # Ordinary Monday
        '08/12/2014',
        '25/12/2014'
    ]    

    def __init__(self, fileName = '/train.csv'):
        self.fileName = fileName
        self.taxiData = []


    def run(self, percentage):
        self.loadFile(percentage)
        self.cleanFile()
        self.buildTimeDimension()
        self.buildLocationDimension()
        self.buildTripDimension()
        self.buildFacts()


    def loadFile(self, percentage = 100):
        if percentage not in range(0, 100):
            percentage = 100

        print('Loading %.2f%% of file %s...' % (percentage, self.fileName))

        # Loading only a percentage of the file
        index = 0
        nbLines = sum(1 for line in open(self.fileName))
        maxLines = int(nbLines * (percentage / 100))
        step = int(maxLines * 0.05)

        with open(self.fileName, newline = '') as csvFile:
            try:
                reader = csv.reader(csvFile, delimiter = ',', quotechar = '"')

                # Preparing object from headers 
                headers = next(reader, None)
                dataRowTemplate = dict((key, None) for (key) in headers)

                for row in reader:
                    dataRow = dict(dataRowTemplate)

                    # Populating a row
                    for i in range(len(headers)):
                        # If line is smaller
                        if i > len(row):
                            break;

                        dataRow[headers[i]] = row[i]

                    self.taxiData.append(dict(dataRow))

                    if index % step == 0:
                        print("%.2f%%" % (float(index) / step * 5))

                    if index > maxLines:
                        break

                    index += 1

            except Exception as e:
                print('Error loading file : %s' % (str(e)))
                return

        print('Done loading file.')



    def print(self, length = 100):
        if length < 0:
            length = abs(length)

        if length == 0:
            length = 1

        print(self.taxiData[:length])



    def __parse(self, data, skipCoordinatesParsing = False):
        headers = ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP', 'DAY_TYPE', 'MISSING_DATA', 'POLYLINE']

        parsedData = {}

        for field in headers:
            if field == 'TRIP_ID':
                parsedData[field] = int(data[field])
            elif field == 'CALL_TYPE':
                parsedData[field] = data[field]
            elif field == 'ORIGIN_CALL':
                if data['CALL_TYPE'] != 'A' or data[field] == '':
                    parsedData[field] = 'NA'
                else:
                    parsedData[field] = data[field]
            elif field == 'ORIGIN_STAND':
                if data['CALL_TYPE'] != 'B' or data[field] == '':
                    parsedData[field] = 'NA'
                else:
                    parsedData[field] = data[field]
            elif field == 'TAXI_ID':
                parsedData[field] = int(data[field])
            elif field == 'TIMESTAMP':
                parsedData[field] = datetime.datetime.fromtimestamp(int(data[field]))
            elif field == 'DAY_TYPE':
                # 'B' if this trip started on a holiday or any other special day (i.e. extending holidays, floating holidays, etc.);
                # 'C' if the trip started on a day before a type-B day;
                # 'A' otherwise (i.e. a normal day, workday or weekend).   

                parsedData[field] = 'A'

                # Checking trip date regarding to holidays
                for stringDate in self.publicHolidays:
                    temp = list(map(int, stringDate.split('/')))
                    holiday = datetime.date(temp[2], temp[1], temp[0])

                    if parsedData['TIMESTAMP'].date() == holiday:
                        parsedData[field] = 'B'
                        break
                    elif parsedData['TIMESTAMP'].date() == holiday - datetime.timedelta(days = 1):
                        parsedData[field] = 'C'
                        break
            elif field == 'MISSING_DATA':
                parsedData[field] = (data[field] == 'True')
            elif field == 'POLYLINE':
                if skipCoordinatesParsing:
                    parsedData[field] = data[field]
                else:
                    parsedData[field] = json.loads(data[field]) # Should be safe (eval() is not), more efficient than literal_eval
                    # parsedData[field] = ast.literal_eval(data[field]) # Should be safe (eval() is not)
            else:
                raise Exception('Unknown field in data...')

        return parsedData



    def cleanFile(self):
        print('Cleaning data...')

        headers = ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP', 'DAY_TYPE', 'MISSING_DATA', 'POLYLINE']

        dataNumber = len(self.taxiData)
        step = int(dataNumber * 0.05)

        for i in range(len(self.taxiData)):
            try:
                self.taxiData[i] = self.__parse(self.taxiData[i])
            except Exception as e:
                print('Error parsing data : %s' % (str(e)))
                continue

            if(i % step == 0):
                print("%.2f%%" % (float(i) / step * 5))

        print('Done cleaning data.')



    def buildTimeDimension(self):
        cluster = None
        session = None

        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect('e21')
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            return

        print('Cleaning previous data...')

        try:
            for table in ['day', 'month', 'day_of_week', 'week', 'day_type']:
                session.execute('TRUNCATE TABLE e21.time_by_%s;' % (table))
        except Exception as e:
            print('Error cleaning database : %s' % (str(e)))
            return

        print('Start building time dimension...')

        # Dealing with dates
        startDate = datetime.datetime(2013, 7, 1) # 01/07/2013
        endDate = datetime.datetime(2014, 6, 30) # 30/06/2014

        # datesList = [startDate + datetime.timedelta(days = d) for d in range(0, (endDate - startDate).days + 1)]
        datesList = [startDate + datetime.timedelta(hours = h) for h in range(0, ((endDate - startDate).days + 1) * 24)]

        dateEntry = {
            'timestamp': None,  # INT
            'date': None,       # TEXT
            'day': None,        # INT
            'month': None,      # INT
            'year': None,       # INT
            'dayOfWeek': None,  # INT
            'weekOfYear': None, # INT
            'hour': None,       # INT
            'minutes': None,    # INT
            'period': None,     # TEXT
            'dayType': None     # TEXT
        }

        index = 0
        nb = len(datesList)
        step = int(nb * 0.05)        

        # Populating data
        for d in datesList:
            entry = dict(dateEntry)

            try:
                entry['timestamp'] = int(time.mktime(d.timetuple()))
                entry['date'] = d.isoformat()
                entry['day'] = int(d.day)
                entry['month'] = int(d.month)
                entry['year'] = int(d.year)
                entry['dayOfWeek'] = int(d.isoweekday())
                entry['weekOfYear'] = int(d.isocalendar()[1])
                entry['hour'] = int(d.hour)
                entry['minutes'] = 0
                entry['period'] = 'None'
                entry['dayType'] = 'A'

                # Checking trip date regarding to holidays
                for stringDate in self.publicHolidays:
                    temp = list(map(int, stringDate.split('/')))
                    holiday = datetime.date(temp[2], temp[1], temp[0])

                    if d.date() == holiday:
                        entry['dayType'] = 'B'
                        break
                    elif d.date() == holiday - datetime.timedelta(days = 1):
                        entry['dayType'] = 'C'
                        break
            except Exception as e:
                print('Error generating dates : %s' % (str(e)))
                continue

            try:
                session.execute(
                    '''
                    INSERT INTO e21.time_by_day (day, month, year, hour, date_timestamp, day_of_week, day_type, minutes, period, week) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (entry['day'], entry['month'], entry['year'], entry['hour'], entry['timestamp'], entry['dayOfWeek'], entry['dayType'], entry['minutes'], entry['period'], entry['weekOfYear'])
                )

                session.execute(
                    '''
                    INSERT INTO e21.time_by_month (day, month, year, hour, date_timestamp, day_of_week, day_type, minutes, period, week) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (entry['day'], entry['month'], entry['year'], entry['hour'], entry['timestamp'], entry['dayOfWeek'], entry['dayType'], entry['minutes'], entry['period'], entry['weekOfYear'])
                )

                session.execute(
                    '''
                    INSERT INTO e21.time_by_day_of_week (day, month, year, hour, date_timestamp, day_of_week, day_type, minutes, period, week) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (entry['day'], entry['month'], entry['year'], entry['hour'], entry['timestamp'], entry['dayOfWeek'], entry['dayType'], entry['minutes'], entry['period'], entry['weekOfYear'])
                )

                session.execute(
                    '''
                    INSERT INTO e21.time_by_week (day, month, year, hour, date_timestamp, day_of_week, day_type, minutes, period, week) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (entry['day'], entry['month'], entry['year'], entry['hour'], entry['timestamp'], entry['dayOfWeek'], entry['dayType'], entry['minutes'], entry['period'], entry['weekOfYear'])
                )                               

                session.execute(
                    '''
                    INSERT INTO e21.time_by_day_type (day, month, year, hour, date_timestamp, day_of_week, day_type, minutes, period, week) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (entry['day'], entry['month'], entry['year'], entry['hour'], entry['timestamp'], entry['dayOfWeek'], entry['dayType'], entry['minutes'], entry['period'], entry['weekOfYear'])
                )                 

                if index % step == 0:
                    print('%.2f%%' % (float(index) / step * 5))

                index += 1
            except Exception as e:
                print('Error inserting date in database : %s' % (str(e)))
                continue

        print('Done building.')



    def buildLocationDimension(self):
        cluster = None
        session = None

        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect('e21')
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            return

        print('Cleaning previous data...')

        try:
            for table in ['zone', 'lon', 'lat']:
                session.execute('TRUNCATE TABLE e21.locations_by_%s;' % table)
        except Exception as e:
            print('Error cleaning database : %s' % (str(e)))
            return

        # Looking for min and max coordonates
        minLon = -9
        maxLon = -8
        minLat = 40.8
        maxLat = 41.5

        # Building the grid
        gridStep = 0.01

        print('Start building location dimension...')

        index = 0
        nb = len(self.taxiData)
        step = int(nb * 0.05)   

        for d in self.taxiData:
            p = d['POLYLINE']

            # Just saving first and last coordonates for now
            if not d['MISSING_DATA'] and len(p) > 0 :
                lonStart = p[0][0]
                latStart = p[0][1]
                lonEnd = p[-1][0]
                latEnd = p[-1][1]

                # Check on coordonates in case of errors ?
                if not (minLon <= lonStart <= maxLon and minLat <= latStart <= maxLat and minLon <= lonEnd <= maxLon and minLat <= latEnd <= maxLat):
                    continue

                try:
                    # By zone (lon and lat)
                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_zone (lat, lon, zone_lat, zone_lon) 
                        VALUES (%s, %s, %s, %s);
                        ''',
                        (latStart, lonStart, round(math.floor(latStart / gridStep) * gridStep, 2), round(math.floor(lonStart / gridStep) * gridStep, 2))
                    )

                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_zone (lat, lon, zone_lat, zone_lon) 
                        VALUES (%s, %s, %s, %s);
                        ''',
                        (latEnd, lonEnd, round(math.floor(latEnd / gridStep) * gridStep, 2), round(math.floor(lonEnd / gridStep) * gridStep, 2))
                    )

                    # By lon
                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_lon (lat, lon, zone_lon) 
                        VALUES (%s, %s, %s);
                        ''',
                        (latStart, lonStart, round(math.floor(lonStart / gridStep) * gridStep, 2))
                    )  

                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_lon (lat, lon, zone_lon) 
                        VALUES (%s, %s, %s);
                        ''',
                        (latEnd, lonEnd, round(math.floor(lonEnd / gridStep) * gridStep, 2))
                    )                                        

                    # By lat
                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_lat (lat, lon, zone_lat) 
                        VALUES (%s, %s, %s);
                        ''',
                        (latStart, lonStart, round(math.floor(latStart / gridStep) * gridStep, 2))
                    )  

                    session.execute(
                        '''
                        INSERT INTO e21.locations_by_lat (lat, lon, zone_lat) 
                        VALUES (%s, %s, %s);
                        ''',
                        (latEnd, lonEnd, round(math.floor(latEnd / gridStep) * gridStep, 2))
                    ) 

                    if index % step == 0:
                      print('%.2f%%' % (float(index) / step * 5))

                    index += 1
                except Exception as e:
                    print('Error inserting location in database : %s' % (str(e)))
                    continue
        
        print('Done building.')



    def buildTripDimension(self):
        cluster = None
        session = None

        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect('e21')
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            return

        print('Cleaning previous data...')

        try:
            for table in ['taxi', 'call_type', 'origin_call', 'origin_stand']:
                session.execute('TRUNCATE TABLE e21.trips_by_%s;' % table)
        except Exception as e:
            print('Error cleaning database : %s' % (str(e)))
            return

        print('Start building trips dimension...')

        index = 0
        nb = len(self.taxiData)
        step = int(nb * 0.05)   

        for d in self.taxiData:
            try:
                session.execute(
                    '''
                    INSERT INTO e21.trips_by_taxi (trip_id, taxi_id, call_type, origin_call, origin_stand)
                    VALUES (%s, %s, %s, %s, %s);
                    ''',
                    (d['TRIP_ID'], d['TAXI_ID'], d['CALL_TYPE'], d['ORIGIN_CALL'], d['ORIGIN_STAND'])
                )
                session.execute(
                    '''
                    INSERT INTO e21.trips_by_call_type (trip_id, taxi_id, call_type, origin_call, origin_stand)
                    VALUES (%s, %s, %s, %s, %s);
                    ''',
                    (d['TRIP_ID'], d['TAXI_ID'], d['CALL_TYPE'], d['ORIGIN_CALL'], d['ORIGIN_STAND'])
                )
                session.execute(
                    '''
                    INSERT INTO e21.trips_by_origin_call (trip_id, taxi_id, call_type, origin_call, origin_stand)
                    VALUES (%s, %s, %s, %s, %s);
                    ''',
                    (d['TRIP_ID'], d['TAXI_ID'], d['CALL_TYPE'], d['ORIGIN_CALL'], d['ORIGIN_STAND'])
                )
                session.execute(
                    '''
                    INSERT INTO e21.trips_by_origin_stand (trip_id, taxi_id, call_type, origin_call, origin_stand)
                    VALUES (%s, %s, %s, %s, %s);
                    ''',
                    (d['TRIP_ID'], d['TAXI_ID'], d['CALL_TYPE'], d['ORIGIN_CALL'], d['ORIGIN_STAND'])
                )                                                

                if index % step == 0:
                  print('%.2f%%' % (float(index) / step * 5))

                index += 1
            except Exception as e:
                print('Error inserting trip in database : %s' % (str(e)))
                continue
        
        print('Done building.')        



    def buildFacts(self):
        cluster = None
        session = None

        try:
            cluster = Cluster(['127.0.0.1'])
            session = cluster.connect('e21')
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            return

        print('Cleaning previous data...')

        try:
            session.execute('TRUNCATE TABLE e21.facts_by_duration;')
            session.execute('TRUNCATE TABLE e21.facts_by_distance;')
        except Exception as e:
            print('Error cleaning database : %s' % (str(e)))
            return

        print('Start building facts...')

        index = 0
        nb = len(self.taxiData)
        step = int(nb * 0.05)   

        for d in self.taxiData:
            p = d['POLYLINE']

            duration = -1
            distance = -1

            lonStart = 0
            latStart = 0
            lonEnd = 0
            latEnd = 0

            if not d['MISSING_DATA'] and len(p) > 1:
                duration = (len(p) - 1) * 15 # One measure every 15s

                distance = sum([Coordonate(i[0], i[1]).distanceFrom(Coordonate(j[0], j[1])) for i, j in zip(p[:-1], p[1:])])

                lonStart = p[0][0]
                latStart = p[0][1]
                lonEnd = p[-1][0]
                latEnd = p[-1][1]                

            try:
                session.execute(
                    '''
                    INSERT INTO e21.facts_by_duration (trip_timestamp, trip_id, lon_start, lat_start, lon_end, lat_end, duration) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (d['TIMESTAMP'], d['TRIP_ID'], lonStart, latStart, lonEnd, latEnd, duration)
                )

                session.execute(
                    '''
                    INSERT INTO e21.facts_by_distance (trip_timestamp, trip_id, lon_start, lat_start, lon_end, lat_end, distance) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (d['TIMESTAMP'], d['TRIP_ID'], lonStart, latStart, lonEnd, latEnd, distance)
                )                

                if index % step == 0:
                  print('%.2f%%' % (float(index) / step * 5))

                index += 1
            except Exception as e:
                print('Error inserting location in database : %s' % (str(e)))
                continue
        
        print('Done building.')


    # def saveTripsByTaxi(self):
    #     cluster = None
    #     session = None

    #     try:
    #         cluster = Cluster(['127.0.0.1'])
    #         session = cluster.connect('e21')
    #     except Exception as e:
    #         print('Error connecting to the database : %s' % (str(e)))
    #         return

    #     try:
    #         session.execute('TRUNCATE TABLE e21.trips_by_taxi;')
    #     except Exception as e:
    #         print('Error cleaning database : %s' % (str(e)))
    #         return

    #     print('Start building trips by taxi...')

    #     index = 0
    #     nb = len(self.taxiData)
    #     step = int(nb * 0.05)

    #     # Iterating trough data
    #     for trip in self.taxiData:
    #         try:
    #             session.execute(
    #                 '''
    #                 INSERT INTO e21.trips_by_taxi (taxi_id, trip_id, trip_timestamp) 
    #                 VALUES (%s, %s, %s, %s, %s);
    #                 ''',
    #                 (trip['TAXI_ID'], str(trip['TRIP_ID']), trip['TIMESTAMP'])
    #             )

    #             if index % step == 0:
    #                 print('%.2f%%' % (float(index) / step * 5))

    #             index += 1
    #         except Exception as e:
    #             print('Error inserting date in database : %s' % (str(e)))
    #             continue

    #     print('Done building.')



    # def saveTripsByCallType(self):
    #     cluster = None
    #     session = None

    #     try:
    #         cluster = Cluster(['127.0.0.1'])
    #         session = cluster.connect('e21')
    #     except Exception as e:
    #         print('Error connecting to the database : %s' % (str(e)))
    #         return

    #     try:
    #         session.execute('TRUNCATE TABLE e21.trips_by_call_type;')
    #     except Exception as e:
    #         print('Error cleaning database : %s' % (str(e)))
    #         return

    #     print('Start building trips by call type...')

    #     index = 0
    #     nb = len(self.taxiData)
    #     step = int(nb * 0.05)

    #     # Iterating trough data
    #     for trip in self.taxiData:
    #         try:
    #             session.execute(
    #                 '''
    #                 INSERT INTO e21.trips_by_call_type (call_type, trip_id, trip_timestamp) 
    #                 VALUES (%s, %s, %s);
    #                 ''',
    #                 (trip['CALL_TYPE'], str(trip['TRIP_ID']), trip['TIMESTAMP'])
    #             )

    #             if index % step == 0:
    #                 print('%.2f%%' % (float(index) / step * 5))

    #             index += 1
    #         except Exception as e:
    #             print('Error inserting date in database : %s' % (str(e)))
    #             continue

    #     print('Done building.')        



    # def saveToDatabase(self):
    #     cluster = None
    #     session = None

    #     try:
    #         cluster = Cluster(['127.0.0.1'])
    #         session = cluster.connect('e21')
    #     except Exception as e:
    #         print('Error connecting to the database : %s' % (str(e)))
    #         return

    #     try:
    #         session.execute('TRUNCATE TABLE e21.dates;')
    #         session.execute('TRUNCATE TABLE e21.locations;')
    #         session.execute('TRUNCATE TABLE e21.trips;')
    #     except Exception as e:
    #         print('Error cleaning database : %s' % (str(e)))
    #         return

    #     # Dealing with dates
    #     startDate = datetime.date(2013, 7, 1) # 01/07/2013
    #     endDate = datetime.date(2014, 6, 30) # 30/06/2014

    #     datesList = [startDate + datetime.timedelta(days = d) for d in range(0, (endDate - startDate).days + 1)]

    #     dateEntry = {
    #         'timestamp': None,  # INT
    #         'date': None,       # TEXT
    #         'day': None,        # INT
    #         'month': None,      # INT
    #         'year': None,       # INT
    #         'dayOfWeek': None,  # INT
    #         'weekOfYear': None, # INT
    #         'hour': None,       # INT
    #         'minutes': None,    # INT
    #         'period': None,     # TEXT
    #         'dayType': None     # TEXT
    #     }

    #     # Populating data
    #     for d in datesList:
    #         entry = dict(dateEntry)

    #         try:
    #             entry['timestamp'] = int(time.mktime(d.timetuple()))
    #             entry['date'] = d.isoformat()
    #             entry['day'] = int(d.day)
    #             entry['month'] = int(d.month)
    #             entry['year'] = int(d.year)
    #             entry['dayOfWeek'] = int(d.isoweekday())
    #             entry['weekOfYear'] = int(d.isocalendar()[1])
    #             entry['hour'] = 0
    #             entry['minutes'] = 0
    #             entry['period'] = 'None'
    #             entry['dayType'] = 'A'

    #              # Checking trip date regarding to holidays
    #             for stringDate in self.publicHolidays:
    #                 temp = list(map(int, stringDate.split('/')))
    #                 holiday = datetime.date(temp[2], temp[1], temp[0])

    #                 if d == holiday:
    #                     entry['dayType'] = 'B'
    #                     break
    #                 elif d == holiday - datetime.timedelta(days = 1):
    #                     entry['dayType'] = 'C'
    #                     break
    #         except Exception as e:
    #             print('Error generating dates : %s' % (str(e)))
    #             continue

    #         # Once everything computed, load to database
    #         try:
    #             session.execute(
    #                 '''
    #                 INSERT INTO e21.dates (tmstp, date, day, month, year, dayOfWeek, weekOfYear, hour, minutes, period, dayType) 
    #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    #                 ''',
    #                 (entry['timestamp'], entry['date'], entry['day'], entry['month'], entry['year'], entry['dayOfWeek'], entry['weekOfYear'], entry['hour'], entry['minutes'], entry['period'], entry['dayType'])
    #             )
    #         except Exception as e:
    #             print('Error inserting date in database : %s' % (str(e)))
    #             continue

    #     # Saving locations and trips
    #     for data in self.taxiData:
    #         # Locations
    #         for coord in data['POLYLINE']: 
    #             try:
    #                 session.execute(
    #                     '''
    #                     INSERT INTO e21.locations (lon, lat, zone)
    #                     VALUES (%s, %s, %s);
    #                     ''',
    #                     (coord[0], coord[1], 0)
    #                 )
    #             except Exception as e:
    #                 print('Error inserting location : %s' % (str(e)))
    #                 continue

    #         # Trips
    #         try:
    #             session.execute(
    #                 '''
    #                 INSERT INTO e21.trips (id, taxi, callType, originCall, originStand, duration, distance)
    #                 VALUES (%s, %s, %s, %s, %s, 0, 0);
    #                 ''',
    #                 (data['TRIP_ID'], data['TAXI_ID'], data['CALL_TYPE'], data['ORIGIN_CALL'], data['ORIGIN_STAND'])
    #             )
    #         except Exception as e:
    #             print('Error inserting trip : %s' % (str(e)))
    #             continue




# If executing file, then run the script
if __name__ == '__main__':
    c = Cleaner('/train.csv')

    percentage = int(input('Pourcentage à charger du fichier : '))

    c.run(percentage)