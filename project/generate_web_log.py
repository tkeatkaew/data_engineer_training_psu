# Web Logs ETL Pipeline: Extract, Transform, Load to MongoDB
# Author: Data Engineering Expert
# Description: Complete ETL pipeline for processing web server logs

import pandas as pd
import numpy as np
import re
from datetime import datetime
import pymongo
from pymongo import MongoClient
import logging
from urllib.parse import urlparse, parse_qs
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WebLogETL:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", db_name="web_analytics"):
        """Initialize ETL pipeline with MongoDB connection"""
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db.web_logs
        
    def create_sample_log_file(self, filename="sample_web_logs.txt", num_records=1000):
        """Create sample web log file in Common Log Format (CLF)"""
        import random
        from datetime import datetime, timedelta
        
        # Sample data for realistic logs
        ips = ["192.168.1.100", "10.0.0.15", "203.0.113.45", "198.51.100.23", "172.16.0.88"]
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
            "curl/7.68.0"
        ]
        paths = ["/", "/products", "/api/users", "/login", "/logout", "/admin", "/images/logo.png", "/css/style.css"]
        methods = ["GET", "POST", "PUT", "DELETE"]
        status_codes = [200, 201, 301, 302, 400, 401, 403, 404, 500, 502]
        
        with open(filename, 'w') as f:
            base_time = datetime.now() - timedelta(days=30)
            for i in range(num_records):
                ip = random.choice(ips)
                timestamp = base_time + timedelta(seconds=random.randint(0, 2592000))  # 30 days
                timestamp_str = timestamp.strftime("%d/%b/%Y:%H:%M:%S +0000")
                method = random.choice(methods)
                path = random.choice(paths)
                if random.random() < 0.1:  # 10% chance of query parameters
                    path += f"?id={random.randint(1,1000)}&page={random.randint(1,10)}"
                status = random.choice(status_codes)
                size = random.randint(100, 50000) if status == 200 else random.randint(0, 1000)
                referer = "-" if random.random() < 0.3 else f"https://example.com{random.choice(paths)}"
                user_agent = random.choice(user_agents)
                
                # Apache Common Log Format with additional fields
                log_line = f'{ip} - - [{timestamp_str}] "{method} {path} HTTP/1.1" {status} {size} "{referer}" "{user_agent}"\n'
                f.write(log_line)
        
        logging.info(f"Created sample log file: {filename} with {num_records} records")
        return filename

    def extract_logs(self, log_file_path):
        """
        EXTRACT: Read and parse web log files
        Supports Apache Common Log Format and Extended Log Format
        """
        logging.info(f"Starting extraction from {log_file_path}")
        
        # Regular expression for Apache Common Log Format with additional fields
        log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-) "(.*?)" "(.*?)"'
        
        logs = []
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    match = re.match(log_pattern, line.strip())
                    if match:
                        ip, timestamp, request, status, size, referer, user_agent = match.groups()
                        logs.append({
                            'ip_address': ip,
                            'timestamp_str': timestamp,
                            'request': request,
                            'status_code': status,
                            'response_size': size,
                            'referer': referer,
                            'user_agent': user_agent,
                            'line_number': line_num
                        })
                    else:
                        logging.warning(f"Failed to parse line {line_num}: {line[:100]}...")
                        
                except Exception as e:
                    logging.error(f"Error processing line {line_num}: {e}")
                    continue
        
        df = pd.DataFrame(logs)
        logging.info(f"Extracted {len(df)} log entries")
        return df

    def transform_logs(self, df):
        """
        TRANSFORM: Clean and transform the log data
        """
        logging.info("Starting data transformation")
        
        # 1. Data Type Conversions
        df['status_code'] = pd.to_numeric(df['status_code'], errors='coerce')
        df['response_size'] = pd.to_numeric(df['response_size'].replace('-', '0'), errors='coerce')
        
        # 2. Parse timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp_str'], format='%d/%b/%Y:%H:%M:%S %z', errors='coerce')
        
        # 3. Extract request components (method, path, protocol)
        request_parts = df['request'].str.extract(r'(\w+)\s+([^\s]+)\s+(HTTP/[\d\.]+)')
        df['method'] = request_parts[0]
        df['path'] = request_parts[1]
        df['protocol'] = request_parts[2]
        
        # 4. Parse URL components
        df['parsed_url'] = df['path'].apply(self._parse_url)
        df['clean_path'] = df['parsed_url'].apply(lambda x: x['path'] if x else None)
        df['query_params'] = df['parsed_url'].apply(lambda x: x['query'] if x else {})
        
        # 5. Categorize file types
        df['file_extension'] = df['clean_path'].str.extract(r'\.([a-zA-Z0-9]+)$')[0]
        df['resource_type'] = df['file_extension'].apply(self._categorize_resource_type)
        
        # 6. Clean and categorize user agents
        df['browser'] = df['user_agent'].apply(self._extract_browser)
        df['os'] = df['user_agent'].apply(self._extract_os)
        df['is_bot'] = df['user_agent'].str.contains('bot|crawler|spider', case=False, na=False)
        
        # 7. Categorize status codes
        df['status_category'] = df['status_code'].apply(self._categorize_status)
        
        # 8. Handle missing values and clean data
        df['referer'] = df['referer'].replace('-', None)
        df['response_size'] = df['response_size'].fillna(0)
        
        # 9. Add derived fields
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.day_name()
        
        # 10. Remove rows with critical missing data
        df_clean = df.dropna(subset=['timestamp', 'ip_address', 'status_code'])
        
        # 11. Data validation
        df_clean = df_clean[df_clean['status_code'].between(100, 599)]
        df_clean = df_clean[df_clean['response_size'] >= 0]
        
        logging.info(f"Transformation complete. Clean records: {len(df_clean)}")
        return df_clean

    def _parse_url(self, url):
        """Parse URL to extract components"""
        try:
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
        
        image_exts = ['jpg', 'jpeg', 'png', 'gif', 'svg', 'ico', 'webp']
        style_exts = ['css', 'scss', 'less']
        script_exts = ['js', 'jsx', 'ts', 'tsx']
        doc_exts = ['pdf', 'doc', 'docx', 'txt']
        
        extension = extension.lower()
        if extension in image_exts:
            return 'image'
        elif extension in style_exts:
            return 'stylesheet'
        elif extension in script_exts:
            return 'script'
        elif extension in doc_exts:
            return 'document'
        else:
            return 'other'

    def _extract_browser(self, user_agent):
        """Extract browser from user agent string"""
        if pd.isna(user_agent):
            return 'Unknown'
        
        browsers = {
            'Chrome': r'Chrome/[\d\.]+',
            'Firefox': r'Firefox/[\d\.]+',
            'Safari': r'Safari/[\d\.]+',
            'Edge': r'Edge/[\d\.]+',
            'Opera': r'Opera/[\d\.]+',
            'curl': r'curl/[\d\.]+',
            'bot': r'(bot|crawler|spider)'
        }
        
        for browser, pattern in browsers.items():
            if re.search(pattern, user_agent, re.IGNORECASE):
                return browser
        return 'Other'

    def _extract_os(self, user_agent):
        """Extract operating system from user agent string"""
        if pd.isna(user_agent):
            return 'Unknown'
        
        os_patterns = {
            'Windows': r'Windows NT [\d\.]+',
            'macOS': r'Mac OS X [\d_\.]+',
            'Linux': r'Linux',
            'iOS': r'iPhone|iPad',
            'Android': r'Android [\d\.]+',
        }
        
        for os_name, pattern in os_patterns.items():
            if re.search(pattern, user_agent, re.IGNORECASE):
                return os_name
        return 'Other'

    def _categorize_status(self, status_code):
        """Categorize HTTP status codes"""
        if pd.isna(status_code):
            return 'Unknown'
        
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

    def load_to_mongodb(self, df, batch_size=1000):
        """
        LOAD: Insert transformed data into MongoDB
        """
        logging.info("Starting data load to MongoDB")
        
        try:
            # Convert DataFrame to dictionary records
            records = df.to_dict('records')
            
            # Convert numpy types to native Python types for MongoDB compatibility
            for record in records:
                for key, value in record.items():
                    if isinstance(value, (np.int64, np.int32)):
                        record[key] = int(value)
                    elif isinstance(value, (np.float64, np.float32)):
                        record[key] = float(value)
                    elif pd.isna(value):
                        record[key] = None
            
            # Insert in batches for better performance
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    result = self.collection.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    logging.info(f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} records")
                except Exception as e:
                    logging.error(f"Error inserting batch {i//batch_size + 1}: {e}")
            
            logging.info(f"Successfully loaded {total_inserted} records to MongoDB")
            
            # Create indexes for better query performance
            self._create_indexes()
            
            return total_inserted
            
        except Exception as e:
            logging.error(f"Error during MongoDB load: {e}")
            raise

    def _create_indexes(self):
        """Create indexes for optimized querying"""
        indexes = [
            [("timestamp", 1)],
            [("ip_address", 1)],
            [("status_code", 1)],
            [("method", 1)],
            [("resource_type", 1)],
            [("date", 1), ("hour", 1)],
            [("browser", 1)],
            [("is_bot", 1)]
        ]
        
        for index in indexes:
            try:
                self.collection.create_index(index)
                logging.info(f"Created index: {index}")
            except Exception as e:
                logging.warning(f"Index creation failed for {index}: {e}")

    def run_etl_pipeline(self, log_file_path=None, create_sample=True):
        """
        Run the complete ETL pipeline
        """
        try:
            # Create sample data if needed
            if create_sample and not log_file_path:
                log_file_path = self.create_sample_log_file()
            
            # Extract
            raw_df = self.extract_logs(log_file_path)
            
            # Transform
            clean_df = self.transform_logs(raw_df)
            
            # Load
            inserted_count = self.load_to_mongodb(clean_df)
            
            logging.info(f"ETL Pipeline completed successfully!")
            logging.info(f"Total records processed: {len(raw_df)}")
            logging.info(f"Clean records: {len(clean_df)}")
            logging.info(f"Records loaded to MongoDB: {inserted_count}")
            
            return {
                'raw_records': len(raw_df),
                'clean_records': len(clean_df),
                'loaded_records': inserted_count,
                'success': True
            }
            
        except Exception as e:
            logging.error(f"ETL Pipeline failed: {e}")
            return {'success': False, 'error': str(e)}

    def get_sample_queries(self):
        """Sample MongoDB queries for analysis"""
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
            'hourly_traffic': list(self.collection.aggregate([
                {'$group': {'_id': '$hour', 'requests': {'$sum': 1}}},
                {'$sort': {'_id': 1}}
            ]))
        }
        return queries

    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()
        logging.info("MongoDB connection closed")

# Example usage
if __name__ == "__main__":
    # Initialize ETL pipeline
    etl = WebLogETL()
    
    # Run the complete pipeline
    result = etl.run_etl_pipeline()
    
    if result['success']:
        print("ETL Pipeline Results:")
        print(f"Raw records: {result['raw_records']}")
        print(f"Clean records: {result['clean_records']}")
        print(f"Loaded records: {result['loaded_records']}")
        
        # Run sample queries
        print("\nSample Analytics:")
        queries = etl.get_sample_queries()
        for query_name, result in queries.items():
            print(f"{query_name}: {result}")
    
    # Close connection
    etl.close_connection()