

# At powershell

pip3 install mysql-connector-python   
pip3 install pymongo
pip3 install pandas


mongosh

# mongosh basic command
show databases
use mongodbVSCodePlaygroundDB
show collections

// Insert a few documents into the sales collection.
db.getCollection('sales').insertMany([
  { 'item': 'abc', 'price': 10, 'quantity': 2, 'date': new Date('2025-06-18T08:00:00Z') },
  { 'item': 'jkl', 'price': 20, 'quantity': 1, 'date': new Date('2025-06-18T09:00:00Z') },
]);

db.sales.find()
db.sales.find({"item":"abc"})

#  At powershell, Import csv file
wget 'https://docs.google.com/uc?export=download&id=1dlzfudHBk-17a1ZGqFkuuXKChXiVbHLm' -O primer-dataset.json

mongoimport --db primerdb --collection primer_dataset --type json --file primer-dataset.json

# At powershell
mongosh

# mongosh command
show databases
use primerdb
show collections
db.primer_dataset.findOne()
db.primer_dataset.find({name: 'Kosher Island'})

// Find documents with zipcode '10314'
db.primer_dataset.find({"address.zipcode": "10314"})

// For prettier output
db.primer_dataset.find({"address.zipcode": "10314"}).pretty()

db.primer_dataset.find({'firstname':'Olive'})















