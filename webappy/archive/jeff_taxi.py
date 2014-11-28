
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import time, datetime
import pandas.io.sql as pdsql
import psycopg2
import cPickle as pickle
import json
from multiprocessing import Pool
import matplotlib.pyplot as plt
from scipy.stats import spearmanr
get_ipython().magic(u'matplotlib inline')


# In[2]:

conn = psycopg2.connect(dbname='ride_viz', user='postgres', host='/tmp')


# In[3]:

get_ipython().magic(u'load_ext sql')
get_ipython().magic(u'sql postgresql://:@localhost/ride_viz')


# In[4]:

df = pdsql.read_sql('SELECT * FROM rs_figures;', conn)


# In[5]:

#Ranges from negative of the longer ride to bigger than positive the longer ride
# df['Extra Time'] = df['osrm_time'] - df[['ctime','mtime']].max(axis=1)
df['Extra Time int'] = df['osrm_time'] - df['ctime'] - df['mtime']
#The bigger the better
df['Percent Extra Time'] = (((df['Extra Time int'] * -1. ) / (df.ctime + df.mtime).map(float)) / 60)
df['Extra Time'] = df['Extra Time int'].apply(lambda x: '%d min %d sec' % ((x / 60) + 2, x % 60))


#Ranges from negative of the longer ride to bigger than positive the longer ride
df['Miles Saved'] = ((df.osrm_dist - df.cdist - df.mdist) * -1 / 1608.) 
#The bigger the better 
df['Percent Mile Saved'] = ((df.osrm_dist - df.cdist - df.mdist) * -1) / (df.cdist + df.mdist)

ride_shares = df[(df['Percent Mile Saved'] > 0) & (df['Percent Extra Time'] > 0)]
ride_shares['Score'] = (((ride_shares['Percent Mile Saved'] * 0.5) + (ride_shares['Percent Extra Time'] * 0.5)) * 100).round(1)
ride_shares['money_saved'] = ride_shares['Miles Saved'].apply(lambda x: ((0.4 * 5) * x + 2.5).round(2))
ride_shares['Money Saved'] = ride_shares['money_saved'].apply(lambda x: '$%.2f' % x )
ride_shares['Time'] = ride_shares['cptime'].apply(lambda x: '%.02d:%.02d:%.02d' % (x.hour, x.minute, x.second))
ride_shares['Miles Saved'] = ride_shares['Miles Saved'].apply(lambda x: x.round(1))


# In[6]:

def strip_city(x) : return x.strip('New York City-')
ride_shares['cpcity'] = ride_shares['cpcity'].apply(strip_city)
ride_shares['mpcity'] = ride_shares['mpcity'].apply(strip_city)
ride_shares['cdcity'] = ride_shares['cdcity'].apply(strip_city)
ride_shares['mdcity'] = ride_shares['mdcity'].apply(strip_city)
ride_shares['cploc'] = ride_shares['cploc'].fillna('(Unknown)')
ride_shares['cdloc'] = ride_shares['cdloc'].fillna('(Unknown)')
ride_shares['mploc'] = ride_shares['mploc'].fillna('(Unknown)')
ride_shares['mdloc'] = ride_shares['mdloc'].fillna('(Unknown)')

ride_shares['point_1'] = ride_shares['cploc'] #ride_shares['cpcity'] + ',' + ride_shares['cpname'] + ',' +
ride_shares['point_2'] = ride_shares['cdloc']#ride_shares['cdcity'] + ',' + ride_shares['cdname'] + ',' +
point_3 = []
point_4 = []
for ind in ride_shares.index:
    row = ride_shares.ix[ind]
    if row['rs_type'] == '1':
        point_3.append('%s' % (row['cdloc']))#row['cdcity'], row['cdname'],
        point_4.append('%s' % (row['mdloc']))#row['mdcity'], row['mdname'], 
    elif row['rs_type'] == '2':
        point_3.append('%s' % (row['mdloc']))#row['mdcity'], row['mdname'], 
        point_4.append('%s' % (row['cdloc']))#row['cdcity'], row['cdname'], 
    else:
        print 'problem'
        break


# In[7]:

ride_shares['point_3'] = point_3
ride_shares['point_4'] = point_4


# In[8]:

del ride_shares['mdloc']
del ride_shares['cdloc']
del ride_shares['mploc']
del ride_shares['cploc']


# In[9]:

print spearmanr(ride_shares['Percent Mile Saved'], ride_shares['Score'])
print spearmanr(ride_shares['Percent Extra Time'], ride_shares['Score'])


# In[10]:

ride_shares.sort(['Score'], inplace=True,ascending=False)


# In[11]:

ride_shares.set_index('c_ride', inplace=True, drop=False)


# In[12]:

l = ['c_ride','mride', 'Miles Saved', 'Extra Time','Money Saved','Score',
     'point_1', 'point_2', 'point_3', 'point_4', 'rs_type', 'money_saved']
ride_share_gps = ride_shares[l].groupby('c_ride')


# In[13]:

matched_rides = []
result = []
skipped = 0
for ride, matched_df in ride_share_gps:
    filtered_df = matched_df[~matched_df.mride.isin(matched_rides)]
    if filtered_df.shape[0]:
        max_score_row = filtered_df[filtered_df.Score == filtered_df.Score.max()]
        result.append(max_score_row.values.tolist()[0])
        matched_rides.append(max_score_row.mride.values[0])
    else:
        skipped += 1
result_df = pd.DataFrame(result, columns=l)


# In[14]:

name_replace_lst = [['Adams Street / Brooklyn Bridge Boulevard', 'Adams Street'],
['Adam Clayton Powell Jr. Boulevard', 'Adam Clayton'],
['Queensboro Bridge (Upper Level],', 'Queensboro Bridge'],
['86th Street Transverse Road', '86th Street Trsv.'],
['79th Street Transverse Road', '79th Street Trsv.'],
['Washington Square South', 'Washington Sq. S.'],
['Washington Square North', 'Washington Sq. N.'],
['Doris C. Freedman Place', 'Doris C. Freedman'],
['Fort Washington Avenue', 'Fort Washington A.'],
['72nd Street Transverse', '72nd Street Trsv.'],
['Washington Square West', 'Washington Sq. W.'],
['Fort Hamilton Parkway', 'Fort Hamilton Pk.']]
be_replace, to_replace_w = zip(*name_replace_lst)

def replaceNm(x):
    if len(x) > 10:
        return x.replace('Street', 'St.')         .replace('Approach', '')         .replace('Parkway', 'Pk.')         .replace('South', 'S.')         .replace('North', 'N.')         .replace('East', 'E.')         .replace('West', 'W.')         .replace('Avenue', 'Ave.')         .replace('Square', 'Sq.')         .replace('Boulevard', 'Bld.')         .replace('Drive', 'D.')         .replace('Place', 'Plc.')
    else:
        return x


# In[15]:

result_df.replace(be_replace, to_replace_w, inplace=True)


# In[16]:

result_df[['point_1', 'point_2', 'point_3', 'point_4']] = result_df[['point_1', 'point_2', 'point_3', 'point_4']].applymap(replaceNm)


# In[17]:

result_df.sort('Score', ascending=False).head()


# In[18]:

result_df.sort('Score', ascending=False, inplace=True)
money_saved = result_df.pop('money_saved')


# In[19]:

def to_datatable_json(df, fname):
    lst_of_lst = df.values.tolist()
#     print lst_of_lst
    #ind = df.index
    #lst_of_dict = [dict(zip(ind, item)) for item in lst_of_lst] 
    d = {'data': lst_of_lst}
    json.dump(d, open('%s' % fname, 'w'))


# In[20]:

to_datatable_json(result_df, 'code/webappy/data/results.json')


# In[21]:

to_datatable_json(result_df.head(2), 'code/webappy/data/testing.json')


# In[22]:

sum(money_saved)


# In[28]:

miles_saved = result_df[~result_df['Extra Time'].str.contains('-')]


# In[29]:

miles_saved['Miles Saved'].mean()


# In[30]:

miles_saved['Miles Saved'].sum()


# In[31]:

df[(df.c_ride == 8116) & (df.mride == 8162)]


# In[44]:

result_df.head()


# In[45]:

def showNames(n):
    result_df['len_name'] = result_df[n].apply(lambda x: len(x))
    a = result_df[[n,'len_name']].sort('len_name', ascending=False)
    print a[n].unique()


# In[46]:

path_lst = ['c_ride', 'mride', 'st_astext', 'cpath', 'mpath']
route_df = ride_shares[path_lst]


# In[47]:

result_df.head()


# In[48]:

route_df['st_astext']  = route_df.st_astext.apply(path_parse)
route_df['cpath'] = route_df.cpath.apply(path_parse)
route_df['mpath'] = route_df.mpath.apply(path_parse)


# In[ ]:

route_df['combined_id'] = route_df.c_ride.map(int).map(str) + ',' + route_df.mride.map(int).map(str)


# In[ ]:

route_df.set_index('combined_id', inplace=True)


# In[ ]:

route_final = route_df[['st_astext', 'cpath', 'mpath']]


# In[ ]:

pwd


# In[137]:

pickle.dump(route_final, open('data/route_final.pkl', 'w'))


# In[104]:

params = ['mride', 'c_ride', 'Time','Miles Saved', 'Extra Time', 'Money Saved']
rs = ride_shares[params]
random.choice(rs.index)


# In[ ]:

#how many ride share
#how many in total
#table map
#applet
#distribution in location
#random forest / logistic regression


# In[101]:

len(ride_shares.c_ride.unique())


# In[87]:

def plt_one_rs(n):
    def path_parse(x):
        lst = x.replace('LINESTRING(', '').replace(')', '').split(',')
        return [item.split(' ') for item in lst]    
    
    x, y = zip(*path_parse(ride_shares.ix[n].st_astext))
    x1, y1 = zip(*path_parse(ride_shares.ix[n].cpath))
    x2, y2 = zip(*path_parse(ride_shares.ix[n].mpath))

    fig, axes = plt.subplots(figsize=(14, 14))
    axes.set_axis_bgcolor('black')
    axes.plot(x, y, lw=10, color='#F08080', alpha=0.4, label='Shared Ride')
    axes.plot(x1,y1, color='#AFEEEE', ls='--', lw=3, label='Ride A')
    axes.plot(x2,y2, color='#55AE3A', ls='--', lw=3, label='Ride B')
    axes.scatter([x[0], x[-1]], [y[0], y[-1]], color='r')
    axes.scatter([x1[0], x1[-1]], [y1[0], y1[-1]], color='b', s=400)
    axes.scatter([x2[0], x2[-1]], [y2[0], y2[-1]], color='g', s=400)


    
    params = ['mride', 'c_ride', 'Time','Miles Saved', 'Extra Time', 'Money Saved']
    print ride_shares.ix[n][params]


# In[105]:

plt_one_rs(27508 )


# In[54]:

def toDataTableFormat(df):
    d = {'data': df.values.tolist()}
    json.dump(d, open('test.json', 'w'))


# In[34]:

toDataTableFormat(sample)


# In[32]:

cd code/webappy/


# In[35]:

sample.head()


# In[33]:

lst = ['osrm_start', 'osrm_end', 'time_diff', 'dist_diff', 'cploc', 'cdloc','mploc', 'mdloc']
df[lst].head()


# In[62]:

get_ipython().run_cell_magic(u'sql', u'', u'select cur_ride, count(*) AS cnt \nFROM first_filter\ngroup by cur_ride\norder by count(*) DESC\nLIMIT 500;')


# In[49]:

7663338 / 11834.


# In[47]:

5559.325 * 7663338 /  1000 / 3600


# In[45]:

get_ipython().run_cell_magic(u'sql', u'', u'SELECT c_ride, mride,\nST_distance(cpath::geography, mpath::geography) AS path_diff,\nST_distance(cploc::geography, mploc::geography) AS pick_diff, \nST_distance(cdloc::geography, mdloc::geography) AS drop_diff, \nST_distance(cploc::geography, mdloc::geography) AS cpick_mdrop_diff,\nST_distance(mploc::geography, cdloc::geography) AS mpick_cdrop_diff,\nST_distance(mpath::geography, cploc::geography) AS mpath_cpick_diff,\nST_distance(cpath::geography, mploc::geography) AS cpath_mpick_diff\nFROM rs_params\nlimit 500')


# In[41]:

get_ipython().run_cell_magic(u'sql', u'', u'SELECT cur.geom AS cpath, cur.pickup_geom AS cploc, cur.dropoff_geom AS cdloc, \ncur.trip_distance AS cdist, cur.trip_time_in_secs AS ctime, cur.total_amount AS cfare,\nmatched.geom AS mpath, matched.pickup_geom AS mploc, matched.dropoff_geom AS mdloc, \nmatched.trip_distance AS mdist, matched.trip_time_in_secs AS mtime, matched.total_amount AS mfare    \nFROM first_filter AS ff\nJOIN rides_11_4_8_all AS cur\nON cur.ride = ff.cur_ride\nJOIN rides_11_4_8_all AS matched\nON matched.ride = ff.ride;\n')


# In[5]:

get_ipython().run_cell_magic(u'sql', u'', u'SELECT nb.county, nb.city, nb.name, \nST_SetSRID(ST_MakePoint(r.pickup_longitude, r.pickup_latitude),4326) AS pickup_geom\nFROM rides AS r\nCROSS JOIN neighborhood_bounds AS nb\nWHERE ST_Within(pickup_geom, nb.geom)\nlimit 5;')


# %%sql
# SELECT row_number() OVER(ORDER BY pickup_datetime,
#                           dropoff_datetime) AS ride, *
# FROM whole
# WHERE passenger_count < 3 AND 
# trip_time_in_secs > 60 AND
# path_dist > 0.5 AND
# EXTRACT(hour FROM pickup_datetime) = 8
# ORDER BY pickup_datetime, dropoff_datetime
# LIMIT 3;

# In[ ]:

get_ipython().run_cell_magic(u'sql', u'', u'select COUNT(*) FROM eight_share_final;')


# In[5]:

get_ipython().run_cell_magic(u'sql', u'', u'CREATE UNIQUE INDEX ride_ind ON eight (ride);')


# In[127]:

get_ipython().run_cell_magic(u'sql', u'', u"CREATE TABLE eight AS\nSELECT row_number() OVER(ORDER BY pickup_datetime,\n                          dropoff_datetime) AS ride, whole.* \nFROM whole\nWHERE passenger_count < 3 AND \ntrip_time_in_secs > 60 AND\npath_dist > 0.5 AND\nEXTRACT(hour FROM pickup_datetime) = 8 AND\npcity = 'New York City-Manhattan' AND\ndcity = 'New York City-Manhattan'\nORDER BY pickup_datetime, dropoff_datetime;")


# In[4]:

get_ipython().run_cell_magic(u'sql', u'', u'SELECT cur_ride, ride, \nST_Distance(ploc::geography, cur_ploc::geography) AS alpha, \nST_Distance(ploc::geography, cur_ploc::geography) AS alpha, \n\nFROM eight_share_final\nWHERE ST_Distance(ploc::geography, cur_ploc::geography) <= 500 LIMIT 1;')


# In[12]:

get_ipython().run_cell_magic(u'sql', u'', u'CREATE TABLE eight_share_final AS\nSELECT es.cur_ride, es.ride,\nec.pickup_datetime AS cur_pdt, ec.dropoff_datetime AS cur_ddt, \nec.pickup_geom AS cur_ploc, ec.dropoff_geom AS cur_dloc,\nec.path_geom AS cur_route, ec.path_dist AS cur_rdis, ec.trip_distance AS cur_dis, ec.trip_time_in_secs AS cur_duration,\n\nem.pickup_datetime AS pdt, em.dropoff_datetime AS ddt, \nem.pickup_geom AS ploc, em.dropoff_geom AS dloc,\nem.path_geom AS route, em.path_dist AS rdis, em.trip_distance AS dis, em.trip_time_in_secs AS duration\n\nFROM eight_share AS es\nJOIN eight AS em\nON em.ride = es.ride\nJOIN eight AS ec\nON ec.ride = es.cur_ride;')


# In[7]:

get_ipython().run_cell_magic(u'sql', u'', u'SELECT es.cur_ride, es.ride,\nec.pickup_datetime AS cur_pdt, ec.dropoff_datetime AS cur_ddt, \nec.pickup_geom AS cur_ploc, ec.dropoff_geom AS cur_dloc,\nec.path_geom AS cur_route, ec.path_dist AS cur_rdis, ec.trip_distance AS cur_dis, ec.trip_time_in_secs AS cur_duration,\n\nem.pickup_datetime AS pdt, em.dropoff_datetime AS ddt, \nem.pickup_geom AS ploc, em.dropoff_geom AS dloc,\nem.path_geom AS route, em.path_dist AS rdis, em.trip_distance AS dis, em.trip_time_in_secs AS duration\n\nFROM eight_share AS es\nJOIN eight AS ec\nON ec.ride = es.cur_ride AND ec.ride = 1\nJOIN eight AS em\nON em.ride = es.ride\nORDER BY cur_ride, ride;')


# In[82]:

whole_q = '''SELECT row_number() OVER(ORDER BY pickup_datetime,
                          dropoff_datetime) AS ride, 
                          pickup_datetime, dropoff_datetime,
                          pickup_longitude AS plng, pickup_latitude AS plat,
                          dropoff_longitude AS dlng, dropoff_latitude AS dlat,
                          pname, dname, (path_dist::FLOAT / 1609.34) AS path_distance, 
                          trip_distance, fare_amount
FROM whole
WHERE passenger_count < 3 AND 
trip_time_in_secs > 60 AND
path_dist > 0.5 AND
EXTRACT(hour FROM pickup_datetime) = 8 AND
pcity = 'New York City-Manhattan' AND
dcity = 'New York City-Manhattan'
ORDER BY pickup_datetime, dropoff_datetime;'''


# In[83]:

df = pdsql.read_sql(whole_q, conn)


# In[84]:

df.set_index('pickup_datetime', inplace=True, drop=False)


# In[124]:

f = open('data/eightShare2Min.csv', 'w')
cnt = 0
for row in df.iterrows():
    s = row[1]
    cur_ride = s.ride
    if cur_ride == 1 or cur_ride % 2000 == 0: print 'Done ', cur_ride
    dfByTime = df.ix[s.pickup_datetime : (s.pickup_datetime + datetime.timedelta(minutes=2))]
    dfByTime['cur_ride'] = cur_ride
    dfByTimeByDistrict = dfByTime[(dfByTime.ride != dfByTime.cur_ride)]
    cnt += dfByTimeByDistrict.shape[0]
    if cur_ride == 1:
        dfByTimeByDistrict.to_csv(f, index=False)
    else:
        dfByTimeByDistrict.to_csv(f, index=False, header=False, mode='a')
f.close()


# In[125]:

cnt


# In[106]:

a = pd.read_csv('data/eightShare2Min.csv')


# In[113]:

a.cur_ride.tail()


# In[111]:

len(a.cur_ride.unique())


# In[7]:

def rolling(ts):
    lower = ts - datetime.timedelta(minutes=5)
    upper = ts + datetime.timedelta(minutes=5)
    return df.ix[lower : upper]


# In[8]:

get_ipython().magic(u'time dfSeries = df.pickup_datetime.apply(rolling)')


# In[ ]:

def work(foo):
    foo.do_task()


pool = Pool()
pool.map(work, my_foo_obj_list)
pool.close()
pool.join()


# In[ ]:

get_ipython().run_cell_magic(u'sql', u'', u"SELECT * \nFROM whole \nWHERE ST_DWithin(geocolumn, 'POINT(1000 1000)', 100.0);")


# 1. SELECT BY geo proximity first
# 2. SELECT BY time

# In[ ]:




# In[11]:

len(dfSeries)


# In[158]:

get_ipython().run_cell_magic(u'time', u'', u'row_cnt = 0\nfor dff in dfSeries:\n    row_cnt += dff.shape[0]\nprint row_cnt')


# In[160]:

824821628 / 5.


# In[176]:

dfSeries[1].shape


# In[ ]:

df_all = pd.concat(dfSeries.tolist())


# In[172]:

get_ipython().run_cell_magic(u'time', u'', u"break_pt = 164964326\nbreak_cnt = 0\nfor ind, fourMinDf in enumerate(dfSeries):\n    fourMinDf['cur_ride'] = ind\n    fourMinDf = fourMinDf[fourMinDf.cur_ride != fourMinDf.ride]\n    if (ind == 0) or (ind % break_pt == 0):\n        fname = 'fourMin_%s.csv' % break_cnt\n        f = open(fname, 'w')\n        print fname, ' created'\n        break_cnt += 1\n        fourMinDf.to_csv(f, header=True)\n    else:\n        fourMinDf.to_csv(f, header=False, mode='a')")


# In[ ]:

ls -l


# In[149]:

get_ipython().system(u'wc fourMin.csv')


# In[64]:

dfSeries[0]


# In[33]:

lower = df.index[10] - datetime.timedelta(minutes=3)
upper = df.index[10] + datetime.timedelta(minutes=3)
df.ix[lower : upper].tail()


# In[23]:

df.head()


# In[6]:

def update_end(df, current, end):
    while end + 1 < len(df) and df.ix[end + 1]['pickup_timestamp'] < df.ix[current]['pickup_timestamp'] + 2 * 60:
        end += 1
    return end

def update_start(df, current, start):
    while start + 1 < len(df) and df.ix[start + 1]['pickup_timestamp'] <= df.ix[current]['pickup_timestamp'] - 2 * 60:
        start = start + 1
    return start

def find_intervals(df):
    start = 0
    end = 0
    results = []
    for current in xrange(len(df)):
        start = update_start(df, current, start)
        end = update_end(df, current, end)
        results.append(df.ix[start:end])
    return results


# In[7]:

get_ipython().magic(u'time results = find_intervals(df)')


# In[11]:

1354.601 * 14000 / 1000 /3600


# In[11]:

from pymongo import MongoClient


# In[12]:

c = MongoClient()
db = c['ride_routes']
tab= db['ride_route_tab']


# In[14]:

a= tab.find_one()


# In[16]:

type(a['ride'])


# In[ ]:



