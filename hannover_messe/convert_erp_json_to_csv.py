#!/usr/bin/env python3
import json
import csv
import re
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_description(desc):
    """Clean description by removing ellipsis and handling special characters"""
    if not desc:
        return ""
    # Remove ellipsis
    desc = desc.replace("...", "")
    # Replace newlines and tabs with spaces
    desc = re.sub(r'[\n\t]+', ' ', desc)
    # Remove multiple spaces
    desc = re.sub(r' +', ' ', desc)
    return desc.strip()

def process_location(location):
    """Split location into city and country using '-' as separator"""
    if not location:
        return "", ""
    
    parts = location.split(" - ", 1)
    city = parts[0].strip() if parts else ""
    country = parts[1].strip() if len(parts) > 1 else ""
    
    return city, country

def process_stand(stand):
    """Split stand into hall and stand number"""
    if not stand:
        return "", ""
    
    hall_match = re.search(r'Halle\s+(\d+[A-Z]?)', stand)
    hall = hall_match.group(1) if hall_match else ""
    
    stand_match = re.search(r'Stand\s+([A-Z]\d+(?:/\d+)?(?:\s*,\s*\([0-9]+\))?)', stand)
    stand_num = stand_match.group(1) if stand_match else ""
    
    return hall, stand_num

def process_product_link(link):
    """Append prefix to product link"""
    if not link:
        return ""
    return f"https://www.hannovermesse.de{link}"

def convert_json_to_csv(json_file, csv_file):
    """Convert the JSON file to CSV with the specified processing"""
    logger.info(f"Converting {json_file} to {csv_file}")
    
    try:
        with open(json_file, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return False
    
    if not data:
        logger.warning("No data found in the JSON file")
        return False
    
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            # Define the CSV columns
            fieldnames = [
                'company_name', 'city', 'country', 'description', 
                'hall', 'stand', 'product_link', 'search_snippet_type'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in data:
                # Process location
                city, country = process_location(item.get('location', ''))
                
                # Process stand
                hall, stand = process_stand(item.get('stand', ''))
                
                # Create the processed row
                row = {
                    'company_name': item.get('company_name', ''),
                    'city': city,
                    'country': country,
                    'description': clean_description(item.get('description', '')),
                    'hall': hall,
                    'stand': stand,
                    'product_link': process_product_link(item.get('product_link', '')),
                    'search_snippet_type': item.get('search_snippet_type', '')
                }
                
                writer.writerow(row)
        
        logger.info(f"Successfully converted to {csv_file}")
        return True
    
    except Exception as e:
        logger.error(f"Error writing to CSV: {e}")
        return False

if __name__ == "__main__":
    input_file = "bde.json"
    output_file = "bde_processed.csv"
    
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} not found")
    else:
        success = convert_json_to_csv(input_file, output_file)
        if success:
            logger.info(f"Conversion completed successfully. Output file: {output_file}")
        else:
            logger.error("Conversion failed")