import matplotlib
matplotlib.use('Agg') # Needed to generate the image from a server

import matplotlib.pyplot as plt

from cassandra.cluster import Cluster
import sys

class Visualizer:
    def __init__(self):
        self.cluster = None
        self.session = None

        try:
            self.cluster = Cluster(['127.0.0.1'])
            self.session = self.cluster.connect('e21')
            self.session.default_timeout = 9999
        except Exception as e:
            print('Error connecting to the database : %s' % (str(e)))
            sys.exit(1)



    def run(self):
        self.getTripsByWeekDays()
        self.getTripsByMonth()

        self.cluster.shutdown()



    def getDistances(self):
        try:
            results = self.session.execute(
                '''
                SELECT truncated_distance, COUNT(*) AS nb
                FROM e21.facts_by_distance
                GROUP BY truncated_distance;
                '''
            )

            y = []
            x = []

            # Going through results
            for r in results:
                if 0 < r.truncated_distance <= 50000:
                    x.append(int(r.truncated_distance))
                    y.append(int(r.nb))

            plt.scatter(x, y)
            plt.xlabel('Distances en m (arrondies par tranche de 100m)')
            plt.ylabel('Effectif')
            plt.title('Classes de distances')

            plt.savefig('distances_by_partition.png')
        except Exception as e:
            print('Error getting distances : %s' % (str(e)))
            return

    def getDurations(self):
        try:
            results = self.session.execute(
                '''
                SELECT truncated_duration, COUNT(*) AS nb
                FROM e21.facts_by_duration
                GROUP BY truncated_duration;
                '''
            )

            x = []
            y = []

            # Going through results
            for r in results:
                if 0 < r.truncated_duration <= 10000:
                    x.append(int(r.truncated_duration))
                    y.append(int(r.nb))                    

            plt.scatter(x, y)
            plt.xlabel('Durées en secondes (arrondies par tranche de 100 secondes)')
            plt.ylabel('Effectif')
            plt.title('Classes de durées')

            plt.savefig('durations_by_partition.png')
        except Exception as e:
            print('Error getting durations : %s' % (str(e)))
            return



    def getTripsByWeekDays(self):
        try:
            results = self.session.execute(
                '''
                SELECT day_of_week, COUNT(*) AS nb
                FROM e21.facts_by_week_day
                GROUP BY year, week, day_of_week;
                '''
            )

            weekDays = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
            values = [0] * 7

            for r in results:
                values[r.day_of_week - 1] += r.nb

            plt.bar(range(len(weekDays)), values, align = 'center')
            plt.xticks(range(len(weekDays)), weekDays)
            plt.xlabel('Jours')
            plt.ylabel('Nombre de trajets')
            plt.title('Influence des jours')

            plt.savefig('trips_by_week_day.png')
        except Exception as e:
            print('Error getting trips by week day : %s' % (str(e)))
            return


    def getTripsByMonth(self):
        try:
            results = self.session.execute(
                '''
                SELECT month, COUNT(*) AS nb
                FROM e21.facts_by_day
                GROUP BY year, month, day;
                '''
            )

            weekDays = ['Jan.', 'Fév.', 'Mars', 'Avril', 'Mai', 'Juin', 'Juil.', 'Août', 'Sept.', 'Oct.', 'Nov.', 'Déc.']
            values = [0] * 12

            for r in results:
                values[r.month - 1] += r.nb

            plt.bar(range(len(weekDays)), values, align = 'center')
            plt.xticks(range(len(weekDays)), weekDays)
            plt.xlabel('Mois')
            plt.ylabel('Nombre de trajets')
            plt.title('Influence des mois')

            plt.savefig('trips_by_month.png')
        except Exception as e:
            print('Error getting trips by month : %s' % (str(e)))
            return            



    def getAverageTripsByDayType(self):
        try:
            dayTypes = {
                'A' : {
                    'nb': 0,
                    'count': 0
                }, 
                'B' : {
                    'nb': 0,
                    'count': 0
                }, 
                'C' : {
                    'nb': 0,
                    'count': 0
                }
            }

            # Getting total number of each day type
            results = self.session.execute(
                '''
                SELECT day, day_type
                FROM e21.facts_by_day
                GROUP BY year, month, day;
                '''                
            )

            for r in results:
                dayTypes[r.day_type]['count'] += 1

            # Getting number of trips
            results = self.session.execute(
                '''
                SELECT day_type, COUNT(*) AS nb
                FROM e21.facts_by_day_type
                GROUP BY year, month, day_type;
                '''
            )

            for r in results:
                dayTypes[r.day_type]['nb'] += r.nb

            # Computing average
            average = {}

            for key in dayTypes.keys():
                if dayTypes[key]['count'] > 0:
                    average[key] = dayTypes[key]['nb'] / dayTypes[key]['count']
                else:
                    average[key] = 0

            plt.bar(range(len(average)), average.values(), align = 'center')
            plt.xticks(range(len(average)), average.keys())
            plt.xlabel('Type de jour')
            plt.ylabel('Nombre de trajets moyen')
            plt.title('Influence des types de jour')

            plt.savefig('average_trips_by_day_type.png')
        except Exception as e:
            print('Error getting average trips by day type : %s' % (str(e)))
            return  



    def getTaxisTop(self, n = 10):
        try:
            results = self.session.execute(
                '''
                SELECT taxi_id, COUNT(*) AS nb
                FROM e21.facts_by_taxi 
                GROUP BY taxi_id;
                '''
            )

            top = [{'id': 0, 'count': -1} for i in range(n)]

            for r in results:
                # Going through top
                for i in range(n):
                    # If we have a max, inserting
                    if r.nb > top[i]['count']:
                        top.insert(i, {'id': r.taxi_id, 'count': r.nb})

                        # Cutting
                        top = top[:n]
                        break;

            print(top)
            return top

        except Exception as e:
            print('Error getting taxis top : %s' % (str(e)))
            return        


# If executing file, then run the script
if __name__ == '__main__':
    v = Visualizer()