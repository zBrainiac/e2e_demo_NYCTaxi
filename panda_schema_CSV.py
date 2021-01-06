import time

import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import DateFormatValidation, MatchesPatternValidation, InListValidation

pattern_id = r'^-?\d{1,16}$'  # Number / integer - up to 16
pattern_dec = r'^-?\d*\.\d{1,2}$'
pattern_geo = r'^-?\d*\.\d{1,20}$'  # geo location / decimal with up to 18 decimal place
pattern_date = r'%Y-%m-%d %H:%M:%S'  # Timestamp yyyy-MM-dd HH:mm:ss (in Zulu/UTC time zone) e.g. 2017-07-01 00:00:07

taxiRide_schema = Schema([
    Column('rideId', [MatchesPatternValidation(pattern_id)]),
    Column('isStart', [InListValidation(['START', 'END'])]),
    Column('endTime', [DateFormatValidation(pattern_date)]),
    Column('startTime', [DateFormatValidation(pattern_date)]),
    Column('startLon', [MatchesPatternValidation(pattern_geo)]),
    Column('startLat', [MatchesPatternValidation(pattern_geo)]),
    Column('endLon', [MatchesPatternValidation(pattern_geo)]),
    Column('endLat', [MatchesPatternValidation(pattern_geo)]),
    Column('passengerCnt', [MatchesPatternValidation(pattern_id)])
], ordered=True)


taxiFare_schema = Schema([
    Column('rideId', [MatchesPatternValidation(pattern_id)]),
    Column('taxiId', [MatchesPatternValidation(pattern_id)]),
    Column('driverId', [MatchesPatternValidation(pattern_id)]),
    Column('startTime', [DateFormatValidation(pattern_date)]),
    Column('paymentType', [InListValidation(['CSH', 'CRD', 'NOC', 'DIS', 'UNK'])]),
    Column('tip', [MatchesPatternValidation(pattern_dec)]),
    Column('tolls', [MatchesPatternValidation(pattern_dec)]),
    Column('totalFare', [MatchesPatternValidation(pattern_dec)])
], ordered=True)

# get data from Taxi Ride data dataset
start_time = time.time()

print('load data from "Taxi Ride" data dataset')

taxiRide_data = pd.read_csv("data/nycTaxiRides_all")
print('taxiRide_data')
print(taxiRide_data)

# Taxi Ride data dataset verification
print('start data verification on "Taxi Ride" data dataset')

taxiRide_data_errors = taxiRide_schema.validate(taxiRide_data)

# print Taxi Ride data dataset verification errors to console
print('validation errors in "Taxi Ride" data dataset')
for taxiRide_data_error in taxiRide_data_errors:
    print(taxiRide_data_error)

# filtering out invalid rows
taxiRide_errors_index_rows = [e.row for e in taxiRide_data_errors]
taxiRide_data_clean = taxiRide_data.drop(index=taxiRide_errors_index_rows)
taxiRide_data_error = taxiRide_data.reindex(index=taxiRide_errors_index_rows)

print("--- %s 'Taxi Ride data cleansing' in seconds ---" % (time.time() - start_time))

#
# get data from Taxi Fare data dataset
#
start_time = time.time()
print('load data from "Taxi Fare" data dataset')

taxiFare_data = pd.read_csv("data/nycTaxiFares_all")
print('taxiFare_data')
print(taxiFare_data)

# Taxi Fare data dataset verification
print('start data verification on "Taxi Fare" data dataset')

taxiFare_data_errors = taxiFare_schema.validate(taxiFare_data)

# print Taxi Fare data dataset verification errors to console
print('validation errors in "Taxi Fare" data dataset')
for taxiFare_data_error in taxiFare_data_errors:
    print(taxiFare_data_error)

# filtering out invalid rows
taxiFare_errors_index_rows = [e.row for e in taxiFare_data_errors]
taxiFare_data_clean = taxiFare_data.drop(index=taxiFare_errors_index_rows)
taxiFare_data_error = taxiFare_data.reindex(index=taxiFare_errors_index_rows)

print("--- %s 'Taxi Fare data cleansing' in seconds ---" % (time.time() - start_time))

#
# some tests
#

# save datasets
taxiRide_data_clean.to_csv('data/taxiRide_data_clean.txt', index=False)
# taxiRide_data_clean.to_csv('data/taxiRide_data_error.txt', index=False)
taxiFare_data_clean.to_csv('data/taxiFare_data_clean.txt', index=False)
# taxiFare_data_error.to_csv('data/taxiFare_data_error.txt', index=False)

#join DF
start_time = time.time()

df_taxiRide_data_clean = pd.DataFrame(taxiRide_data_clean)
df_taxiRide_data_clean.columns = ['rideId', 'isStart', 'endTime', 'startTime', 'startLon', 'startLat', 'endLon',
                                  'endLat', 'passengerCnt']

df_taxiFare_data_clean = pd.DataFrame(taxiFare_data_clean)
df_taxiFare_data_clean.columns = ['rideId', 'taxiId', 'driverId', 'startTime', 'paymentType', 'tip', 'tolls',
                                  'totalFare']

print("First 10 rows of the 'df_taxiFare_data_clean: '")
print(df_taxiFare_data_clean.iloc[:10])

df_inner_joined_total = df_taxiRide_data_clean.join(df_taxiFare_data_clean, lsuffix='_rideId', how='inner')
print("First 10 rows of the 'df_inner_joined_total: '")
print(df_inner_joined_total.columns.values)
print(df_inner_joined_total)
print("--- %s 'Joined cleansing Taxi Ride and Fare data' in seconds ---" % (time.time() - start_time))
df_inner_joined_total.to_csv('data/joinedRideFare.txt', index=False)

