# Fixed Web Logs ETL Pipeline with MongoDB Integration
# This version includes better error handling and debugging

import pandas as pd
import numpy as np
import re
from datetime import datetime
import pymongo
from pymongo import MongoClient
import logging
from urllib.parse import urlparse, parse_qs
import json
import sys

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_debug.log')
    ]
)

class FixedWebLogETL:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="web_analytics"):
        """Initialize ETL pipeline with MongoDB connection"""
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            self.collection = self.db.web_logs
            logging.info(f"✅ Successfully connected to MongoDB: {db_name}")
        except Exception as e:
            logging.error(f"❌ Failed to connect to MongoDB: {e}")
            logging.error("Make sure MongoDB is running and accessible")
            raise

    def extract_logs(self, log_file_path):
        """
        EXTRACT: Read and parse web log files with improved error handling
        """
        logging.info(f"🔍 Starting extraction from {log_file_path}")
        
        # Improved regex pattern for Apache Common Log Format
        log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-) "(.*?)" "(.*?)"'
        
        logs = []
        failed_lines = []
        
        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as file:
                for line_num, line in enumerate(file, 1):
                    # Remove carriage returns and whitespace
                    line = line.strip().replace('\r', '')
                    
                    if not line:  # Skip empty lines
                        continue
                        
                    try:
                        match = re.match(log_pattern, line)
                        if match:
                            ip, timestamp, request, status, size, referer, user_agent = match.groups()
                            logs.append({
                                'ip_address': ip.strip(),
                                'timestamp_str': timestamp.strip(),
                                'request': request.strip(),
                                'status_code': status.strip(),
                                'response_size': size.strip(),
                                'referer': referer.strip(),
                                'user_agent': user_agent.strip(),
                                'line_number': line_num
                            })
                        else:
                            failed_lines.append((line_num, line[:100]))
                            logging.warning(f"⚠️  Failed to parse line {line_num}: {line[:100]}...")
                            
                    except Exception as e:
                        failed_lines.append((line_num, str(e)))
                        logging.error(f"❌ Error processing line {line_num}: {e}")
                        continue
        
        except FileNotFoundError:
            logging.error(f"❌ File not found: {log_file_path}")
            raise
        except Exception as e:
            logging.error(f"❌ Error reading file: {e}")
            raise
        
        df = pd.DataFrame(logs)
        
        logging.info(f"✅ Extraction complete:")
        logging.info(f"   📊 Successfully parsed: {len(df)} records")
        logging.info(f"   ⚠️  Failed to parse: {len(failed_lines)} lines")
        
        if len(df) == 0:
            logging.error("❌ No valid log entries found!")
            return pd.DataFrame()
            
        return df

    def transform_logs(self, df):
        """
        TRANSFORM: Clean and transform the log data with detailed validation
        """
        if df.empty:
            logging.error("❌ Empty DataFrame received for transformation")
            return pd.DataFrame()
            
        logging.info(f"🔄 Starting transformation of {len(df)} records")
        original_count = len(df)
        
        try:
            # 1. Data Type Conversions with error handling
            logging.info("📝 Converting data types...")
            df['status_code'] = pd.to_numeric(df['status_code'], errors='coerce')
            df['response_size'] = pd.to_numeric(df['response_size'].replace('-', '0'), errors='coerce')
            
            # 2. Parse timestamp with multiple format attempts
            logging.info("🕐 Parsing timestamps...")
            def parse_timestamp(ts_str):
                formats = [
                    '%d/%b/%Y:%H:%M:%S %z',
                    '%d/%b/%Y:%H:%M:%S +0000',
                    '%d/%b/%Y:%H:%M:%S'
                ]
                for fmt in formats:
                    try:
                        return pd.to_datetime(ts_str, format=fmt)
                    except:
                        continue
                return None
            
            df['timestamp'] = df['timestamp_str'].apply(parse_timestamp)
            
            # 3. Extract request components
            logging.info("🔍 Parsing HTTP requests...")
            request_pattern = r'(\w+)\s+([^\s]+)\s+(HTTP/[\d\.]+)'
            request_parts = df['request'].str.extract(request_pattern)
            df['method'] = request_parts[0]
            df['path'] = request_parts[1]
            df['protocol'] = request_parts[2]
            
            # 4. Parse URL components
            logging.info("🌐 Parsing URLs...")
            df['parsed_url'] = df['path'].apply(self._parse_url)
            df['clean_path'] = df['parsed_url'].apply(lambda x: x['path'] if x else None)
            df['query_params'] = df['parsed_url'].apply(lambda x: json.dumps(x['query']) if x and x['query'] else '{}')
            
            # 5. Categorize file types
            logging.info("📁 Categorizing resources...")
            df['file_extension'] = df['clean_path'].str.extract(r'\.([a-zA-Z0-9]+)$')[0]
            df['resource_type'] = df['file_extension'].apply(self._categorize_resource_type)
            
            # 6. Process user agents
            logging.info("🖥️  Processing user agents...")
            df['browser'] = df['user_agent'].apply(self._extract_browser)
            df['os'] = df['user_agent'].apply(self._extract_os)
            df['is_bot'] = df['user_agent'].str.contains('bot|crawler|spider', case=False, na=False)
            
            # 7. Categorize status codes
            logging.info("📊 Categorizing status codes...")
            df['status_category'] = df['status_code'].apply(self._categorize_status)
            
            # 8. Handle missing values
            logging.info("🧹 Cleaning data...")
            df['referer'] = df['referer'].replace('-', None)
            df['response_size'] = df['response_size'].fillna(0)
            
            # 9. Add derived fields
            logging.info("📈 Adding derived fields...")
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.day_name()
            
            # 10. Data validation and cleaning
            logging.info("✅ Validating data...")
            before_validation = len(df)
            
            # Remove rows with critical missing data
            df_clean = df.dropna(subset=['timestamp', 'ip_address', 'status_code']).copy()
            
            # Validate status codes and response sizes
            df_clean = df_clean[df_clean['status_code'].between(100, 599, na=False)]
            df_clean = df_clean[df_clean['response_size'] >= 0]
            
            after_validation = len(df_clean)
            removed_count = before_validation - after_validation
            
            logging.info(f"✅ Transformation complete:")
            logging.info(f"   📊 Original records: {original_count}")
            logging.info(f"   🧹 Records after cleaning: {after_validation}")
            logging.info(f"   🗑️  Records removed: {removed_count}")
            
            return df_clean
            
        except Exception as e:
            logging.error(f"❌ Error during transformation: {e}")
            raise

    def _parse_url(self, url):
        """Parse URL to extract components"""
        try:
            if pd.isna(url):
                return None
            parsed = urlparse(url)
            return {
                'path': parsed.path,
                'query': parse_qs(parsed.query) if parsed.query else {}
            }
        except:
            return None

    def _categorize_resource_type(self, extension):
        """Categorize file types"""
        if pd.isna(extension):
            return 'page'
        
        extension = str(extension).lower()
        
        if extension in ['jpg', 'jpeg', 'png', 'gif', 'svg', 'ico', 'webp']:
            return 'image'
        elif extension in ['css', 'scss', 'less']:
            return 'stylesheet'
        elif extension in ['js', 'jsx', 'ts', 'tsx']:
            return 'script'
        elif extension in ['pdf', 'doc', 'docx', 'txt']:
            return 'document'
        else:
            return 'other'

    def _extract_browser(self, user_agent):
        """Extract browser from user agent string"""
        if pd.isna(user_agent):
            return 'Unknown'
        
        user_agent = str(user_agent).lower()
        
        if 'chrome' in user_agent:
            return 'Chrome'
        elif 'firefox' in user_agent:
            return 'Firefox'
        elif 'safari' in user_agent and 'chrome' not in user_agent:
            return 'Safari'
        elif 'edge' in user_agent:
            return 'Edge'
        elif 'opera' in user_agent:
            return 'Opera'
        elif 'curl' in user_agent:
            return 'curl'
        elif any(bot in user_agent for bot in ['bot', 'crawler', 'spider']):
            return 'Bot'
        else:
            return 'Other'

    def _extract_os(self, user_agent):
        """Extract operating system from user agent string"""
        if pd.isna(user_agent):
            return 'Unknown'
        
        user_agent = str(user_agent).lower()
        
        if 'windows' in user_agent:
            return 'Windows'
        elif 'mac os x' in user_agent or 'macintosh' in user_agent:
            return 'macOS'
        elif 'linux' in user_agent:
            return 'Linux'
        elif 'iphone' in user_agent or 'ipad' in user_agent:
            return 'iOS'
        elif 'android' in user_agent:
            return 'Android'
        else:
            return 'Other'

    def _categorize_status(self, status_code):
        """Categorize HTTP status codes"""
        if pd.isna(status_code):
            return 'Unknown'
        
        try:
            status_code = int(status_code)
            if 200 <= status_code < 300:
                return 'Success'
            elif 300 <= status_code < 400:
                return 'Redirection'
            elif 400 <= status_code < 500:
                return 'Client Error'
            elif 500 <= status_code < 600:
                return 'Server Error'
            else:
                return 'Unknown'
        except:
            return 'Unknown'

    def load_to_mongodb(self, df, batch_size=1000):
        """
        LOAD: Insert transformed data into MongoDB with comprehensive error handling
        """
        if df.empty:
            logging.error("❌ No data to load - DataFrame is empty")
            return 0
            
        logging.info(f"📤 Starting MongoDB load of {len(df)} records")
        
        try:
            # Test MongoDB connection
            self.client.admin.command('ping')
            logging.info("✅ MongoDB connection verified")
            
            # Clear existing data (optional)
            existing_count = self.collection.count_documents({})
            if existing_count > 0:
                logging.info(f"🗑️  Found {existing_count} existing records")
                response = input("Clear existing data? (y/n): ").lower()
                if response == 'y':
                    self.collection.delete_many({})
                    logging.info("🗑️  Cleared existing data")
            
            # Convert DataFrame to dictionary records
            logging.info("🔄 Converting DataFrame to MongoDB documents...")
            records = df.to_dict('records')
            
            # Clean records for MongoDB compatibility
            for record in records:
                for key, value in record.items():
                    if isinstance(value, (np.int64, np.int32)):
                        record[key] = int(value)
                    elif isinstance(value, (np.float64, np.float32)):
                        if np.isnan(value):
                            record[key] = None
                        else:
                            record[key] = float(value)
                    elif pd.isna(value):
                        record[key] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        record[key] = value.isoformat() if value else None
                    elif isinstance(value, pd.Timestamp):
                        record[key] = value.to_pydatetime() if not pd.isna(value) else None
            
            # Insert in batches
            total_inserted = 0
            failed_inserts = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    result = self.collection.insert_many(batch, ordered=False)
                    batch_inserted = len(result.inserted_ids)
                    total_inserted += batch_inserted
                    logging.info(f"✅ Batch {batch_num}: Inserted {batch_inserted} records")
                    
                except Exception as e:
                    failed_inserts += len(batch)
                    logging.error(f"❌ Batch {batch_num} failed: {e}")
                    
                    # Try inserting records one by one to identify problematic records
                    for j, record in enumerate(batch):
                        try:
                            self.collection.insert_one(record)
                            total_inserted += 1
                        except Exception as single_error:
                            logging.error(f"❌ Failed to insert record {i+j+1}: {single_error}")
                            logging.error(f"   Problematic record: {record}")
            
            logging.info(f"📊 Load Summary:")
            logging.info(f"   ✅ Successfully inserted: {total_inserted} records")
            logging.info(f"   ❌ Failed inserts: {failed_inserts} records")
            
            if total_inserted > 0:
                # Create indexes for better query performance
                self._create_indexes()
                
                # Verify the data was inserted
                final_count = self.collection.count_documents({})
                logging.info(f"🔍 Verification: {final_count} total records in MongoDB")
            
            return total_inserted
            
        except Exception as e:
            logging.error(f"❌ Critical error during MongoDB load: {e}")
            raise

    def _create_indexes(self):
        """Create indexes for optimized querying"""
        indexes_to_create = [
            [("timestamp", 1)],
            [("ip_address", 1)],
            [("status_code", 1)],
            [("method", 1)],
            [("resource_type", 1)],
            [("browser", 1)],
            [("status_category", 1)]
        ]
        
        logging.info("📇 Creating database indexes...")
        created_count = 0
        
        for index in indexes_to_create:
            try:
                index_name = f"idx_{'_'.join([field for field, _ in index])}"
                self.collection.create_index(index, name=index_name)
                created_count += 1
            except Exception as e:
                logging.warning(f"⚠️  Index creation failed for {index}: {e}")
        
        logging.info(f"✅ Created {created_count} database indexes")

    def run_etl_pipeline(self, log_file_path="sample_web_logs.txt"):
        """
        Run the complete ETL pipeline with comprehensive error handling
        """
        pipeline_start = datetime.now()
        logging.info("🚀 Starting ETL Pipeline...")
        
        try:
            # Extract
            logging.info("=" * 50)
            logging.info("PHASE 1: EXTRACT")
            logging.info("=" * 50)
            raw_df = self.extract_logs(log_file_path)
            
            if raw_df.empty:
                logging.error("❌ No data extracted. Pipeline stopped.")
                return {'success': False, 'error': 'No data extracted'}
            
            # Transform
            logging.info("=" * 50)
            logging.info("PHASE 2: TRANSFORM")
            logging.info("=" * 50)
            clean_df = self.transform_logs(raw_df)
            
            if clean_df.empty:
                logging.error("❌ No data after transformation. Pipeline stopped.")
                return {'success': False, 'error': 'No data after transformation'}
            
            # Load
            logging.info("=" * 50)
            logging.info("PHASE 3: LOAD")
            logging.info("=" * 50)
            inserted_count = self.load_to_mongodb(clean_df)
            
            # Pipeline completion
            pipeline_end = datetime.now()
            duration = pipeline_end - pipeline_start
            
            logging.info("=" * 50)
            logging.info("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logging.info("=" * 50)
            logging.info(f"📊 Pipeline Summary:")
            logging.info(f"   ⏱️  Duration: {duration}")
            logging.info(f"   📥 Raw records extracted: {len(raw_df)}")
            logging.info(f"   🧹 Clean records after transformation: {len(clean_df)}")
            logging.info(f"   💾 Records loaded to MongoDB: {inserted_count}")
            logging.info(f"   📈 Success rate: {(inserted_count/len(raw_df)*100):.1f}%")
            
            return {
                'success': True,
                'raw_records': len(raw_df),
                'clean_records': len(clean_df),
                'loaded_records': inserted_count,
                'duration': str(duration),
                'success_rate': round(inserted_count/len(raw_df)*100, 1)
            }
            
        except Exception as e:
            pipeline_end = datetime.now()
            duration = pipeline_end - pipeline_start
            
            logging.error("=" * 50)
            logging.error("💥 ETL PIPELINE FAILED!")
            logging.error("=" * 50)
            logging.error(f"❌ Error: {e}")
            logging.error(f"⏱️  Duration before failure: {duration}")
            
            return {
                'success': False, 
                'error': str(e),
                'duration': str(duration)
            }

    def get_sample_queries(self):
        """Sample MongoDB queries for data verification"""
        try:
            logging.info("🔍 Running sample queries...")
            
            queries = {
                'total_requests': self.collection.count_documents({}),
                'unique_visitors': len(self.collection.distinct('ip_address')),
                'status_distribution': list(self.collection.aggregate([
                    {'$group': {'_id': '$status_category', 'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}}
                ])),
                'top_pages': list(self.collection.aggregate([
                    {'$match': {'resource_type': 'page'}},
                    {'$group': {'_id': '$clean_path', 'visits': {'$sum': 1}}},
                    {'$sort': {'visits': -1}},
                    {'$limit': 10}
                ])),
                'browser_distribution': list(self.collection.aggregate([
                    {'$group': {'_id': '$browser', 'count': {'$sum': 1}}},
                    {'$sort': {'count': -1}}
                ]))
            }
            
            logging.info("✅ Sample queries completed successfully")
            return queries
            
        except Exception as e:
            logging.error(f"❌ Error running sample queries: {e}")
            return {}

    def close_connection(self):
        """Close MongoDB connection"""
        try:
            self.client.close()
            logging.info("✅ MongoDB connection closed")
        except Exception as e:
            logging.error(f"❌ Error closing MongoDB connection: {e}")

# Example usage with debugging
if __name__ == "__main__":
    print("🚀 Web Logs ETL Pipeline - Enhanced Debug Version")
    print("=" * 60)
    
    try:
        # Initialize ETL pipeline
        etl = FixedWebLogETL()
        
        # Run the complete pipeline
        result = etl.run_etl_pipeline("sample_web_logs.txt")
        
        if result['success']:
            print(f"\n🎉 SUCCESS! Pipeline completed in {result['duration']}")
            print(f"📊 Loaded {result['loaded_records']} records to MongoDB")
            
            # Run sample queries to verify data
            print("\n🔍 Running verification queries...")
            queries = etl.get_sample_queries()
            
            if queries:
                print(f"   📈 Total requests: {queries.get('total_requests', 0)}")
                print(f"   👥 Unique visitors: {queries.get('unique_visitors', 0)}")
                print(f"   📊 Status categories: {len(queries.get('status_distribution', []))}")
                print(f"   🖥️  Browser types: {len(queries.get('browser_distribution', []))}")
        else:
            print(f"\n💥 FAILED! Error: {result['error']}")
            
    except Exception as e:
        print(f"\n💥 CRITICAL ERROR: {e}")
        print("Please check that MongoDB is running and accessible")
        
    finally:
        try:
            etl.close_connection()
        except:
            pass
    
    print("\n📋 Check 'etl_debug.log' for detailed logs")