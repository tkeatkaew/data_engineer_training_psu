#!/usr/bin/env python3
"""
Simple ETL Runner - Specifically for your sample_web_logs.txt file
This script will process your exact log file and load it to MongoDB
"""

import pandas as pd
import re
from datetime import datetime
from pymongo import MongoClient
import json

def process_log_file(filename="sample_web_logs.txt"):
    """Process the web log file and return a clean DataFrame"""
    print(f"📂 Processing log file: {filename}")
    
    # Read the file
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        print(f"✅ Read {len(lines)} lines from file")
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        return None
    
    # Parse each line
    log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-) "(.*?)" "(.*?)"'
    records = []
    
    for line_num, line in enumerate(lines, 1):
        # Clean the line (remove carriage returns and whitespace)
        clean_line = line.strip().replace('\r', '')
        
        if not clean_line:  # Skip empty lines
            continue
        
        # Parse with regex
        match = re.match(log_pattern, clean_line)
        if match:
            ip, timestamp, request, status, size, referer, user_agent = match.groups()
            
            # Parse the request to get method and path
            request_parts = request.split(' ', 2)  # Split into 3 parts max
            method = request_parts[0] if len(request_parts) > 0 else 'UNKNOWN'
            path = request_parts[1] if len(request_parts) > 1 else '/'
            protocol = request_parts[2] if len(request_parts) > 2 else 'HTTP/1.1'
            
            # Parse timestamp (convert to Python datetime)
            try:
                # Handle the timezone format
                timestamp_clean = timestamp.replace(' +0000', '')
                dt = datetime.strptime(timestamp_clean, '%d/%b/%Y:%H:%M:%S')
            except:
                dt = None
            
            # Create record
            record = {
                'line_number': line_num,
                'ip_address': ip,
                'timestamp_str': timestamp,
                'timestamp': dt,
                'method': method,
                'path': path,
                'protocol': protocol,
                'status_code': int(status),
                'response_size': int(size) if size != '-' else 0,
                'referer': referer if referer != '-' else None,
                'user_agent': user_agent,
                'processed_at': datetime.now()
            }
            
            # Add some derived fields
            if dt:
                record['date'] = dt.date()
                record['hour'] = dt.hour
                record['day_of_week'] = dt.strftime('%A')
            
            # Categorize status codes
            if 200 <= record['status_code'] < 300:
                record['status_category'] = 'Success'
            elif 300 <= record['status_code'] < 400:
                record['status_category'] = 'Redirect'
            elif 400 <= record['status_code'] < 500:
                record['status_category'] = 'Client Error'
            elif 500 <= record['status_code'] < 600:
                record['status_category'] = 'Server Error'
            else:
                record['status_category'] = 'Other'
            
            # Simple browser detection
            ua_lower = user_agent.lower()
            if 'chrome' in ua_lower:
                record['browser'] = 'Chrome'
            elif 'firefox' in ua_lower:
                record['browser'] = 'Firefox'
            elif 'safari' in ua_lower:
                record['browser'] = 'Safari'
            elif 'curl' in ua_lower:
                record['browser'] = 'curl'
            else:
                record['browser'] = 'Other'
            
            records.append(record)
        else:
            print(f"⚠️  Could not parse line {line_num}: {clean_line[:50]}...")
    
    # Create DataFrame
    df = pd.DataFrame(records)
    print(f"✅ Successfully parsed {len(df)} records")
    
    if len(df) > 0:
        print("📊 Data summary:")
        print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"   Unique IPs: {df['ip_address'].nunique()}")
        print(f"   Status codes: {sorted(df['status_code'].unique())}")
        print(f"   Methods: {sorted(df['method'].unique())}")
        print(f"   Browsers: {sorted(df['browser'].unique())}")
    
    return df

def save_to_mongodb(df, mongo_uri="mongodb://localhost:27017/", db_name="web_analytics", collection_name="web_logs"):
    """Save DataFrame to MongoDB"""
    print(f"\n💾 Saving to MongoDB...")
    print(f"   URI: {mongo_uri}")
    print(f"   Database: {db_name}")
    print(f"   Collection: {collection_name}")
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        print("✅ MongoDB connection successful")
        
        # Get database and collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Check if collection already has data
        existing_count = collection.count_documents({})
        if existing_count > 0:
            print(f"⚠️  Collection already has {existing_count} documents")
            choice = input("Delete existing data? (y/n): ").lower().strip()
            if choice == 'y':
                collection.delete_many({})
                print("🗑️  Existing data deleted")
            else:
                print("📝 Will append to existing data")
        
        # Convert DataFrame to records
        records = df.to_dict('records')
        
        # Clean records for MongoDB (handle NaN values and data types)
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    record[key] = value
                elif hasattr(value, 'isoformat'):  # date objects
                    record[key] = value.isoformat()
        
        # Insert records
        result = collection.insert_many(records)
        print(f"✅ Inserted {len(result.inserted_ids)} records")
        
        # Verify the insertion
        final_count = collection.count_documents({})
        print(f"✅ Total records in collection: {final_count}")
        
        # Create some basic indexes
        print("📇 Creating indexes...")
        try:
            collection.create_index("ip_address")
            collection.create_index("timestamp")
            collection.create_index("status_code")
            collection.create_index("method")
            print("✅ Indexes created")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")
        
        # Show some sample queries
        print("\n📊 Sample statistics:")
        
        # Status code distribution
        status_dist = list(collection.aggregate([
            {"$group": {"_id": "$status_code", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))
        print(f"   Status codes: {status_dist[:5]}")  # Top 5
        
        # Method distribution
        method_dist = list(collection.aggregate([
            {"$group": {"_id": "$method", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))
        print(f"   HTTP methods: {method_dist}")
        
        # Top IPs
        ip_dist = list(collection.aggregate([
            {"$group": {"_id": "$ip_address", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 3}
        ]))
        print(f"   Top IPs: {ip_dist}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return False

def main():
    """Main function to run the ETL process"""
    print("🚀 Simple Web Log ETL Process")
    print("=" * 50)
    
    # Step 1: Process the log file
    df = process_log_file("sample_web_logs.txt")
    
    if df is None or len(df) == 0:
        print("💥 No data to process. Exiting.")
        return
    
    # Step 2: Save to MongoDB
    success = save_to_mongodb(df)
    
    if success:
        print("\n🎉 ETL Process completed successfully!")
        print("✅ Data is now available in MongoDB")
        print("\n📋 Next steps:")
        print("   1. Connect to MongoDB: mongo")
        print("   2. Use database: use web_analytics")
        print("   3. View data: db.web_logs.find().limit(5)")
        print("   4. Count records: db.web_logs.count()")
    else:
        print("\n💥 ETL Process failed!")
        print("🔧 Troubleshooting:")
        print("   1. Make sure MongoDB is running")
        print("   2. Check MongoDB logs for errors")
        print("   3. Try connecting manually: mongo --host localhost:27017")

if __name__ == "__main__":
    main()