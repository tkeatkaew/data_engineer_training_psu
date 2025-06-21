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

    def run_etl_pipeline(self, log_file_path=None, create_sample=True):
        """
        Run the complete ETL pipeline
        """
        try:
            # Create sample data if needed
            if create_sample and not log_file_path:
                log_file_path = self.create_sample_log_file()

             return {
                'raw_records': len(raw_df),
                'clean_records': len(clean_df),
                'loaded_records': inserted_count,
                'success': True
            }
        
                       
# Example usage
if __name__ == "__main__":
    # Initialize ETL pipeline
    etl = WebLogETL()
    
    # Run the complete pipeline
    result = etl.run_etl_pipeline()
    