import csv
from cassandra.cluster import Cluster
import datetime
import time
# import ast
import json
import sys
from coordonates import Coordonate


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
        # '05/05/2013', # Ordinary Sunday
        # '09/05/2013', # Regional holiday
        # '30/05/2013', # Ordinary Thursday
        '10/06/2013',
        # '13/06/2013', # Regional holiday
        # '24/06/2013', # Regional holiday
        # '29/06/2013', # Regional holiday
        '15/08/2013',
        # '05/10/2013', # Ordinary Saturday
        # '01/11/2013', # Ordinary Friday
        # '01/12/2013', # Ordinary Sunday
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
        # '04/05/2014', # Ordinary Sunday
        # '29/05/2014', # Regional holiday
        '10/06/2014',
        # '13/06/2014', # Regional holiday
        # '19/06/2014', # Ordinay Thursday
        # '24/06/2014', # Regional holiday
        '29/06/2014', # Regional holiday
        '15/08/2014',
        # '05/10/2014', # Ordinary Sunday
        # '01/11/2014', # Ordinary Saturday
        # '01/12/2014', # Ordinary Monday
        '08/12/2014',
        '25/12/2014'
    ]    

    def __init__(self, fileName = '/train.csv'):
        self.fileName = fileName
        self.cluster = None
        self.executionStep = 1 # In percentage
        self.tables = ['day', 'start_zone', 'arrival_zone', 'taxi', 'distance', 'duration', 'week_day', 'day_type']

        try:
            self.cluster = Cluster(['127.0.0.1'])
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            sys.exit(1)



    def run(self, percentage):
        # Connecting to database
        session = None
        
        try:
            session = self.cluster.connect('e21')
            session.default_timeout = 9999 # 60 seconds timeout   
        except Exception as e:
            print('Error connecting to the cluster : %s' % (str(e)))
            return

        # Creating table if needed
        self.__createTables(session)

        # Running
        self.__processFile(session, percentage)

        # Once it's done
        self.cluster.shutdown()


    def __cleanTables(self, session):
        try:
            for table in self.tables:
                session.execute('TRUNCATE TABLE e21.facts_by_%s;' % table)
                pass

            return True
        except Exception as e:
            print('Error cleaning database : %s' % (str(e)))
            return False                


    def __processFile(self, session, percentage = 100):
        if percentage not in range(0, 100):
            percentage = 100

        percentage = round(percentage)

        print('Cleaning previous data...')

        # if not self.__cleanTables(session):
        #     return

        print('Processing %d%% of file %s...' % (percentage, self.fileName))

        # Loading only a percentage of the file
        index = 0
        nbLines = sum(1 for line in open(self.fileName))
        maxLines = int(nbLines * (percentage / 100))
        step = int(maxLines * (self.executionStep / 100))

        nbErrors = 0

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

                    # Processing
                    if not self.__processLine(dataRow, session):
                        nbErrors += 1

                    if index % step == 0:
                        print("%.2f%%" % (float(index) / step * self.executionStep), end = '\r')

                    if index > maxLines:
                        break

                    index += 1

            except Exception as e:
                print('Error processing file : %s' % (str(e)))
                return

        print('Done Processing file (errors %.2f%%).' % (float(nbErrors) / (index + 1) * 100))


    # Process a file line (clean, and insert)
    def __processLine(self, line, session):
        try:
            line = self.__parseData(line)
        except Exception as e:
            print('Error parsing data : %s' % (str(e)))
            return False

        return self.__insertFact(line, session)


    # Clean a data row
    def __parseData(self, data, skipCoordinatesParsing = False):
        headers = ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP', 'DAY_TYPE', 'MISSING_DATA', 'POLYLINE']

        cleanedData = {}

        for field in headers:
            if field == 'TRIP_ID':
                cleanedData[field] = int(data[field])
            elif field == 'CALL_TYPE':
                cleanedData[field] = data[field]
            elif field == 'ORIGIN_CALL':
                if data['CALL_TYPE'] != 'A' or data[field] == '':
                    cleanedData[field] = 'NA'
                else:
                    cleanedData[field] = data[field]
            elif field == 'ORIGIN_STAND':
                if data['CALL_TYPE'] != 'B' or data[field] == '':
                    cleanedData[field] = 'NA'
                else:
                    cleanedData[field] = data[field]
            elif field == 'TAXI_ID':
                cleanedData[field] = int(data[field])
            elif field == 'TIMESTAMP':
                cleanedData[field] = datetime.datetime.fromtimestamp(int(data[field]))
            elif field == 'DAY_TYPE':
                # 'B' if this trip started on a holiday or any other special day (i.e. extending holidays, floating holidays, etc.);
                # 'C' if the trip started on a day before a type-B day;
                # 'A' otherwise (i.e. a normal day, workday or weekend).   

                cleanedData[field] = 'A'

                # Checking trip date regarding to holidays
                for stringDate in self.publicHolidays:
                    temp = list(map(int, stringDate.split('/')))
                    holiday = datetime.date(temp[2], temp[1], temp[0])

                    if cleanedData['TIMESTAMP'].date() == holiday:
                        cleanedData[field] = 'B'
                        break
                    elif cleanedData['TIMESTAMP'].date() == holiday - datetime.timedelta(days = 1):
                        cleanedData[field] = 'C'
                        break
            elif field == 'MISSING_DATA':
                cleanedData[field] = (data[field] == 'True')
            elif field == 'POLYLINE':
                if skipCoordinatesParsing:
                    cleanedData[field] = data[field]
                else:
                    cleanedData[field] = json.loads(data[field]) # Should be safe (eval() is not), more efficient than literal_eval
            else:
                raise Exception('Unknown field in data...')

        return cleanedData



    def __insertFact(self, data, session):
        taxiID = data['TAXI_ID']      
        
        time = {
            'year': int(data['TIMESTAMP'].year), # As a datetime because data was cleaned before
            'month': int(data['TIMESTAMP'].month),
            'day': int(data['TIMESTAMP'].day),
            'hour': int(data['TIMESTAMP'].hour),
            'minutes': int(data['TIMESTAMP'].minute),
            'seconds': int(data['TIMESTAMP'].second),
            'dayOfWeek': int(data['TIMESTAMP'].isoweekday()),
            'week': int(data['TIMESTAMP'].isocalendar()[1]),
            'period': 'NA', # Could be an attribute saying 'morning', 'afternoon', etc.
            'dayType': data['DAY_TYPE']
        }

        trip = {
            'id': data['TRIP_ID'],
            'originCall': data['ORIGIN_CALL'],
            'originStand': data['ORIGIN_STAND'],
            'callType': data['CALL_TYPE'],
        }


        startLon = 0
        startLat = 0
        arrivalLon = 0
        arrivalLat = 0

        duration = -1
        distance = -1        

        # Looking for min and max coordonates
        minLon = -8.8
        maxLon = -8
        minLat = 40.8
        maxLat = 41.5          

        roundNumber = 3 # Rounding to x decimals

        # If coordonate is ok
        if not data['MISSING_DATA'] and len(data['POLYLINE']) > 1:
            p = data['POLYLINE']

            duration = (len(p) - 1) * 15 # One measure every 15 seconds

            if minLon <= p[0][0] <= maxLon: 
                startLon = p[0][0]

            if minLat <= p[0][1] <= maxLat:
                startLat = p[0][1]

            if minLon <= p[-1][0] <= maxLon: 
                arrivalLon = p[-1][0]
              
            if minLat <= p[-1][1] <= maxLat:
                arrivalLat = p[-1][1]

            if startLon != 0 and startLat != 0 and arrivalLon != 0 and arrivalLat != 0:
                distance = sum([Coordonate(i[0], i[1]).distanceFrom(Coordonate(j[0], j[1])) for i, j in zip(p[:-1], p[1:])])

        startLocation = {
            'zone': Coordonate(round(startLon, roundNumber), round(startLat, roundNumber)),
            'place': Coordonate(startLon, startLat)
        }

        arrivalLocation = {
            'zone': Coordonate(round(arrivalLon, roundNumber), round(arrivalLat, roundNumber)),
            'place': Coordonate(arrivalLon, arrivalLat)
        }

        # Inserting data in database        
        try:
            session.execute(
                '''
                INSERT INTO e21.facts_by_week_day (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            )            
            session.execute(
                '''
                INSERT INTO e21.facts_by_day_type (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            )
            session.execute(
                '''
                INSERT INTO e21.facts_by_day (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            )
            session.execute(
                '''
                INSERT INTO e21.facts_by_start_zone (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            )
            session.execute(
                '''
                INSERT INTO e21.facts_by_arrival_zone (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            ) 
            session.execute(
                '''
                INSERT INTO e21.facts_by_taxi (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance)
            )
            session.execute(
                '''
                INSERT INTO e21.facts_by_distance (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance, truncated_distance)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance, int(round(distance, -2))) # Rounding to 100 meters
            )
            session.execute(
                '''
                INSERT INTO e21.facts_by_duration (start_zone_lon, start_zone_lat, arrival_zone_lon, arrival_zone_lat, taxi_id, trip_id, call_type, origin_call, origin_stand, year, month, day, hour, minutes, seconds, day_of_week, week, period, day_type, start_lon, start_lat, arrival_lon, arrival_lat, duration, distance, truncated_duration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (startLocation['zone'].lon, startLocation['zone'].lat, arrivalLocation['zone'].lon, arrivalLocation['zone'].lat, taxiID, trip['id'], trip['callType'], trip['originCall'], trip['originStand'], time['year'], time['month'], time['day'], time['hour'], time['minutes'], time['seconds'], time['dayOfWeek'], time['week'], time['period'], time['dayType'], startLocation['place'].lon, startLocation['place'].lat, arrivalLocation['place'].lon, arrivalLocation['place'].lat, duration, distance, int(round(duration, -2))) # Rounding to 100 seconds (around 2min)
            )

            return True                                                            
        except Exception as e:
            print('Error inserting data : %s' % (str(e)))
            return False


    def __createTables(self, session):
        try:
            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_day (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT, 
                    PRIMARY KEY ((year, month, day), hour, minutes, seconds, trip_id)
                );
                '''
            )
            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_start_zone (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT,
                    PRIMARY KEY ((start_zone_lon, start_zone_lat), year, month, day, minutes, seconds, trip_id)
                );
                '''
            )
            session.execute(
                '''        
                CREATE TABLE IF NOT EXISTS e21.facts_by_arrival_zone (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT, 
                    PRIMARY KEY ((arrival_zone_lon, arrival_zone_lat), year, month, day, minutes, seconds, trip_id)
                );
                '''
            )
            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_taxi (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT, 
                    PRIMARY KEY (taxi_id, year, month, day, minutes, seconds, trip_id)
                );
                '''
            )
            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_distance (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT,
                    truncated_distance INT, 
                    PRIMARY KEY (truncated_distance, distance, year, month, day, minutes, seconds, trip_id)
                );
                '''
            )
            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_duration (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT,
                    truncated_duration INT, 
                    PRIMARY KEY (truncated_duration, duration, year, month, day, minutes, seconds, trip_id)
                );                
                '''
            )

            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_week_day (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT, 
                    PRIMARY KEY ((year, week, day_of_week), hour, minutes, seconds, trip_id)
                );                
                '''
            )     

            session.execute(
                '''
                CREATE TABLE IF NOT EXISTS e21.facts_by_day_type (
                    start_zone_lon FLOAT,
                    start_zone_lat FLOAT,
                    arrival_zone_lon FLOAT,
                    arrival_zone_lat FLOAT,
                    taxi_id INT,
                    trip_id BIGINT,
                    call_type TEXT,
                    origin_call TEXT,
                    origin_stand TEXT,  
                    year INT,
                    month INT,
                    day INT,
                    hour INT,
                    minutes INT,
                    seconds INT,
                    day_of_week INT,
                    week INT,
                    period TEXT,
                    day_type TEXT,  
                    start_lon FLOAT,
                    start_lat FLOAT,
                    arrival_lon FLOAT,
                    arrival_lat FLOAT,
                    duration INT,
                    distance FLOAT, 
                    PRIMARY KEY ((year, month, day_type), hour, minutes, seconds, trip_id)
                );                
                '''
            )                   
        except Exception as e:
            print('Error creating tables : %s' % (str(e)))
            self.cluster.shutdown()
            sys.exit(2)


# If executing file, then run the script
if __name__ == '__main__':
    c = Cleaner('/train.csv')

    percentage = int(input('Percentage of file to load : '))

    c.run(percentage)