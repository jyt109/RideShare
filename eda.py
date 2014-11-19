import pandas as pd
import pandas.io.sql as pdsql
import psycopg2
import matplotlib.pyplot as plt
import seaborn

#All queries here are based on one day's worth of data
conn = psycopg2.connect(dbname='nyc_rs', user='postgres', host='/tmp')
imgPath = '../'

def stripNewYork(cell):
    return cell.replace('New York City-', '').strip()

#Dropoffs and Pickups by district
def dropPickByDistrict(conn):
    drop_pick_q = '''WITH pickcnt AS (
                    SELECT pcity, COUNT(*) AS pc
                    FROM whole
                    GROUP BY pcity
                ), dropcnt AS (
                    SELECT dcity, COUNT(*) AS dc
                    FROM whole
                    GROUP BY dcity
                )
                SELECT pickcnt.pcity AS city, pc AS pickups, dc AS dropoffs, (pc + dc) AS total
                FROM pickcnt
                JOIN dropcnt
                ON pickcnt.pcity = dropcnt.dcity
                ORDER BY total DESC;'''
    drop_pick = pdsql.read_sql(drop_pick_q, conn)
    drop_pick['city'] = drop_pick.city.apply(stripNewYork)
    drop_pick.set_index('city', inplace=True)

    fig, axes = plt.subplots(figsize=(10, 6))
    drop_pick[['pickups', 'dropoffs']].plot(kind='bar', ax=axes);
    axes.set_ylabel('Number of Pickups / Dropoffs')
    fig.savefig('../images/dropoffPickupsBar.png', dpi=500, pad_inches=3.2)
    return drop_pick

#Dropoffs and Pickups by district and time
def dropPickByDistrictTime(conn):
    drop_pick_time_q = '''WITH pickcnt AS (
                        SELECT pcity, EXTRACT(hour FROM pickup_datetime) AS hr, COUNT(*) AS pc
                        FROM whole
                        GROUP BY pcity, hr
                    ), dropcnt AS (
                        SELECT dcity, EXTRACT(hour FROM dropoff_datetime) AS hr, COUNT(*) AS dc
                        FROM whole
                        GROUP BY dcity, hr
                    )
                    SELECT pickcnt.pcity AS city, pickcnt.hr AS hr, pc, dc, (pc + dc) AS total
                    FROM pickcnt
                    JOIN dropcnt
                    ON pickcnt.pcity = dropcnt.dcity AND
                    pickcnt.hr = dropcnt.hr
                    ORDER BY city, hr;'''
    drop_pick_time = pdsql.read_sql(drop_pick_time_q, conn)
    drop_pick_time['city'] = drop_pick_time.city.apply(stripNewYork)
    drop_pick_time.set_index('city', inplace=True)
    manhattan = 'Manhattan'
    other_cities = ['Brooklyn','Queens','Bronx']

    def plotTotalByDistrictTime(manhattan, other_cities, drop_pick_time=drop_pick_time):
        fig1, axes1 = plt.subplots(figsize=(15, 5))
        df = drop_pick_time.ix[manhattan]
        axes1.plot(df.hr, df.total, label=manhattan, color='cyan')
        axes1.set_ylabel('Number of Rides')
        axes1.legend(bbox_to_anchor=(-.05, 1), loc='upper right', borderaxespad=1)
        axes1.set_xlabel('Time (Hour)')

        axes2 = axes1.twinx()
        for city in other_cities:
            df = drop_pick_time.ix[city]
            axes2.plot(df.hr, df.total, label=city)
        axes2.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
        axes1.grid()
        axes2.grid()

        fig1.set_size_inches(17.5, 5)
        fig1.savefig('../images/trafficByDistrictTime.png', dpi=500)

    def plotBrooklynManhattanDropPick(drop_pick_time=drop_pick_time):
        fig1, axes1 = plt.subplots(figsize=(15, 5))
        man = drop_pick_time.ix['Manhattan']
        brook = drop_pick_time.ix['Brooklyn']
        axes1.plot(man.hr, man.pc, label='Manhattan pickup', ls='dashed', color='r')
        axes1.plot(man.hr, man.dc, label='Manhattan dropoff', ls='dashed', color='g')
        axes1.legend(bbox_to_anchor=(.4, 1), loc=2, borderaxespad=0)
        axes1.set_xlabel('Time (Hour)')
        axes1.set_ylabel('Number of Manhattan pickup / dropoff')

        axes2 = axes1.twinx()
        axes2.plot(brook.hr, brook.pc, label='Brooklyn pickup', color='r', alpha=0.4)
        axes2.plot(brook.hr, brook.dc, label='Brooklyn dropoff', color='g', alpha=0.4)
        axes2.legend(bbox_to_anchor=(.55, 1), loc=2, borderaxespad=0)
        axes2.set_ylabel('Number of Brooklyn pickup / dropoff')

        axes1.grid()
        axes2.grid()

        fig1.set_size_inches(20, 5)
        fig1.savefig('../images/dropPickBrooklynManhattan.png', dpi=500)



# def dropPickBigDistrictGrpBy(conn):
#     big_dis_q = "SELECT pcity, dcity, \
#                 EXTRACT(hour FROM pickup_datetime) AS hr, COUNT(*) AS cnt \
#                 FROM whole \
#                 GROUP BY pcity, dcity, hr \
#                 ORDER BY pcity, dcity DESC;"
#     big_district_cnts = pdsql.read_sql(big_dis_q, conn)

# def dropPickDistrictGrpBy(conn):
#     big_dis_q = "SELECT pcity, dcity, \
#                 EXTRACT(hour FROM pickup_datetime) AS hr, COUNT(*) AS cnt \
#                 FROM whole \
#                 GROUP BY pcity, dcity, hr \
#                 ORDER BY pcity, dcity DESC;"

#     big_district_cnts = pdsql.read_sql(big_dis_q, conn)

# def plot_route():



def plot_pick_drop(merged):
    figsize(15, 15)
    plat = first_day.pickup_latitude
    plong = first_day.pickup_longitude
    dlat = first_day.dropoff_latitude
    dlong = first_day.dropoff_longitude

    plt.scatter(plat, plong, alpha=0.3)
    plt.scatter(dlat, dlong, alpha=0.3, color='r')

