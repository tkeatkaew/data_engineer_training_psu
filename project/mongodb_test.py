#!/usr/bin/env python3
"""
Simple MongoDB Connection Test
Run this to diagnose MongoDB connection issues
"""

import sys

def test_imports():
    """Test if required modules can be imported"""
    print("🔍 Testing Python imports...")
    
    try:
        import pymongo
        print(f"✅ pymongo version: {pymongo.__version__}")
    except ImportError:
        print("❌ pymongo not found. Install with: pip install pymongo")
        return False
    
    try:
        import pandas as pd
        print(f"✅ pandas version: {pd.__version__}")
    except ImportError:
        print("❌ pandas not found. Install with: pip install pandas")
        return False
    
    return True

def test_mongodb_connection():
    """Test MongoDB connection with multiple configurations"""
    print("\n🔍 Testing MongoDB connections...")
    
    try:
        from pymongo import MongoClient
        from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
    except ImportError:
        print("❌ Cannot import pymongo")
        return None
    
    # Different connection strings to try
    connection_attempts = [
        ("Local Default", "mongodb://localhost:27017/"),
        ("Local IP", "mongodb://127.0.0.1:27017/"),
        ("Local Direct", "mongodb://localhost:27017/?directConnection=true"),
        ("Local No Auth", "mongodb://localhost:27017/?authSource=admin"),
    ]
    
    for name, uri in connection_attempts:
        print(f"\n🔄 Trying: {name}")
        print(f"   URI: {uri}")
        
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            
            # Test the connection
            result = client.admin.command('ping')
            print("   ✅ Connection successful!")
            
            # Get server info
            server_info = client.server_info()
            print(f"   📊 MongoDB version: {server_info.get('version', 'unknown')}")
            
            # List databases
            databases = client.list_database_names()
            print(f"   📚 Databases: {databases}")
            
            # Test write operation
            test_db = client.connection_test
            test_collection = test_db.test
            
            # Insert test document
            test_doc = {"test": "connection", "status": "ok"}
            result = test_collection.insert_one(test_doc)
            print(f"   ✅ Test insert successful: {result.inserted_id}")
            
            # Clean up test data
            test_collection.delete_one({"_id": result.inserted_id})
            print("   🗑️  Test data cleaned up")
            
            client.close()
            return uri
            
        except ServerSelectionTimeoutError:
            print("   ❌ Connection timeout (MongoDB not running?)")
        except ConnectionFailure:
            print("   ❌ Connection failed")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def test_minimal_etl():
    """Test minimal ETL process with in-memory data"""
    print("\n🔍 Testing minimal ETL process...")
    
    try:
        import pandas as pd
        import re
        from datetime import datetime
        from pymongo import MongoClient
        
        # Test data processing (without file)
        sample_logs = [
            '203.0.113.45 - - [04/Jun/2025:05:15:41 +0000] "PUT /products HTTP/1.1" 404 63 "-" "Mozilla/5.0"',
            '192.168.1.100 - - [12/Jun/2025:17:26:34 +0000] "GET /css/style.css HTTP/1.1" 500 138 "-" "Mozilla/5.0"'
        ]
        
        print("✅ Sample log data created")
        
        # Parse logs
        log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-) "(.*?)" "(.*?)"'
        parsed_records = []
        
        for line in sample_logs:
            match = re.match(log_pattern, line)
            if match:
                ip, timestamp, request, status, size, referer, user_agent = match.groups()
                
                # Parse request
                request_parts = request.split(' ')
                method = request_parts[0] if len(request_parts) > 0 else 'UNKNOWN'
                path = request_parts[1] if len(request_parts) > 1 else '/'
                
                record = {
                    'ip_address': ip,
                    'timestamp_str': timestamp,
                    'method': method,
                    'path': path,
                    'status_code': int(status),
                    'response_size': int(size) if size != '-' else 0,
                    'user_agent': user_agent,
                    'processed_at': datetime.now().isoformat()
                }
                parsed_records.append(record)
        
        print(f"✅ Parsed {len(parsed_records)} records")
        
        # Create DataFrame
        df = pd.DataFrame(parsed_records)
        print(f"✅ Created DataFrame with {len(df)} rows")
        
        # Test MongoDB insertion
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)
        db = client.test_etl_minimal
        collection = db.test_logs
        
        # Clear any existing data
        collection.delete_many({})
        
        # Convert DataFrame to records and insert
        records = df.to_dict('records')
        result = collection.insert_many(records)
        
        print(f"✅ Inserted {len(result.inserted_ids)} records to MongoDB")
        
        # Verify insertion
        count = collection.count_documents({})
        print(f"✅ Verified {count} records in database")
        
        # Show a sample record
        sample = collection.find_one()
        if sample:
            print("✅ Sample record from MongoDB:")
            for key, value in sample.items():
                if key != '_id':
                    print(f"   {key}: {value}")
        
        # Clean up
        collection.delete_many({})
        client.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Minimal ETL test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_mongodb_service():
    """Check if MongoDB service is running"""
    print("\n🔍 Checking MongoDB service status...")
    
    import subprocess
    import platform
    
    system = platform.system().lower()
    
    try:
        if system == "windows":
            # Check Windows service
            result = subprocess.run(['sc', 'query', 'MongoDB'], 
                                  capture_output=True, text=True)
            if "RUNNING" in result.stdout:
                print("✅ MongoDB Windows service is running")
                return True
            else:
                print("❌ MongoDB Windows service not running")
                print("💡 Try: net start MongoDB")
                
        elif system == "linux":
            # Check Linux systemd service
            result = subprocess.run(['systemctl', 'is-active', 'mongod'], 
                                  capture_output=True, text=True)
            if result.stdout.strip() == "active":
                print("✅ MongoDB Linux service is running")
                return True
            else:
                print("❌ MongoDB Linux service not running")
                print("💡 Try: sudo systemctl start mongod")
                
        elif system == "darwin":  # macOS
            # Check macOS brew service
            result = subprocess.run(['brew', 'services', 'list'], 
                                  capture_output=True, text=True)
            if "mongodb-community" in result.stdout and "started" in result.stdout:
                print("✅ MongoDB macOS service is running")
                return True
            else:
                print("❌ MongoDB macOS service not running")
                print("💡 Try: brew services start mongodb-community")
        
    except Exception as e:
        print(f"⚠️  Could not check service status: {e}")
    
    # Try to check if process is running
    try:
        if system in ["linux", "darwin"]:
            result = subprocess.run(['pgrep', 'mongod'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ MongoDB process found running")
                return True
        elif system == "windows":
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq mongod.exe'], 
                                  capture_output=True, text=True)
            if "mongod.exe" in result.stdout:
                print("✅ MongoDB process found running")
                return True
    except:
        pass
    
    print("❌ MongoDB process not found")
    return False

def main():
    """Main diagnostic function"""
    print("🔧 MongoDB ETL Diagnostic Tool")
    print("=" * 50)
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    
    # Step 1: Test imports
    if not test_imports():
        print("\n💥 Import test failed - install missing packages")
        return
    
    # Step 2: Check MongoDB service
    mongodb_running = check_mongodb_service()
    
    # Step 3: Test MongoDB connection
    working_uri = test_mongodb_connection()
    
    if not working_uri:
        print("\n💥 MongoDB connection failed!")
        print("\n🔧 Troubleshooting steps:")
        
        if not mongodb_running:
            print("1. ⚠️  MongoDB service is not running")
            print("   Windows: net start MongoDB")
            print("   Linux:   sudo systemctl start mongod")
            print("   macOS:   brew services start mongodb-community")
            print("   Docker:  docker run -d -p 27017:27017 mongo")
        
        print("2. 🔌 Check if MongoDB is listening on port 27017:")
        print("   netstat -an | grep 27017")
        
        print("3. 📦 If MongoDB is not installed:")
        print("   Visit: https://docs.mongodb.com/manual/installation/")
        
        return
    
    # Step 4: Test minimal ETL
    if test_minimal_etl():
        print("\n🎉 All tests passed!")
        print(f"✅ Working MongoDB URI: {working_uri}")
        print("✅ Data processing: OK")
        print("✅ MongoDB operations: OK")
        print("✅ ETL pipeline: Ready")
        print("\n🚀 You can now run the full ETL pipeline!")
    else:
        print("\n💥 ETL test failed despite MongoDB connection working")
        print("This might be a permissions or data format issue")

if __name__ == "__main__":
    main()