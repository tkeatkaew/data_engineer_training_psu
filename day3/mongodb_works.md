

# At powershell
mongosh

# mongosh basic command
show databases
use mongodbVSCodePlaygroundDB
show collections

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















