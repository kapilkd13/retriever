from retriever import interface

dataset='iris' # default dataset is iris
backend='csv'  # default backend is csv

#  Script list
print("List of all available scripts")
print(interface.list())


# Checking Install function

# for csv
print("Installing to csv file")
interface.install(dataset,'csv')

# for json
print("Installing to a json file")
interface.install(dataset,'json')

# for mysql
print("Installing to a mysql database")
interface.install(dataset,'mysql',conn_file='./Connection.conn')

# for sqlite
print("Installing to a sqlite database")
interface.install(dataset,'sqlite',db_file='./sqlite.db')



# Checking Fetch function

# for csv
dataframe= interface.fetch(dataset,'csv')
print(" This data is fetched from csv ")
print(dataframe)

# for json
dataframe= interface.fetch(dataset,'json')
print(" This data is fetched from json ")
print(dataframe)

# for mysql
dataframe= interface.fetch(dataset,'mysql',conn_file='./Connection.conn')
print(" This data is fetched from mysql ")
print(dataframe)

# for sqlite
dataframe= interface.fetch(dataset,'sqlite')
print(" This data is fetched from sqlite ")
print(dataframe)
