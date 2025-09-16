#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - REST API Version
Uses Shopify REST API for reliable data fetching
Matches Shopify's exact JSON format
"""

import os
import json
import requests
from datetime import datetime
import sys
import glob
import time

class ShopifyRESTFetcher:
    def __init__(self, shop_domain, access_token):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = "2023-10"
        self.base_url = f"https://{shop_domain}/admin/api/{self.api_version}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
    def test_connection(self):
        """Test API connection"""
        print("Testing API connection...", file=sys.stderr)
        
        url = f"{self.base_url}/shop.json"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            shop_data = response.json()["shop"]
            print(f"Connected to: {shop_data['name']}", file=sys.stderr)
            print(f"Currency: {shop_data['currency']}", file=sys.stderr)
            print(f"Domain: {shop_data['domain']}", file=sys.stderr)
            return True
        else:
            print(f"Failed to connect. Status: {response.status_code}", file=sys.stderr)
            return False
    
    def fetch_all_products(self):
        """Fetch all products using REST API with pagination"""
        print("\n=== Fetching Products ===", file=sys.stderr)
        
        all_products = []
        limit = 250
        page_info = None
        
        while True:
            # Build URL with pagination
            if page_info:
                url = f"{self.base_url}/products.json?limit={limit}&page_info={page_info}"
            else:
                url = f"{self.base_url}/products.json?limit={limit}"
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching products: {response.status_code}", file=sys.stderr)
                break
            
            data = response.json()
            products = data.get("products", [])
            
            if not products:
                break
                
            all_products.extend(products)
            print(f"Fetched {len(products)} products. Total: {len(all_products)}", file=sys.stderr)
            
            # Check for next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header:
                # Extract page_info from Link header
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        page_info = link.split("page_info=")[1].split(">")[0]
                        break
            else:
                break
            
            # Rate limiting
            time.sleep(0.5)
        
        # Save in Shopify format
        output = {"products": all_products}
        self.save_json("products.json", output)
        
        print(f"Saved {len(all_products)} products to products.json", file=sys.stderr)
        return all_products
    
    def fetch_all_collections(self):
        """Fetch all smart and custom collections"""
        print("\n=== Fetching Collections ===", file=sys.stderr)
        
        all_collections = []
        
        # Fetch smart collections
        print("Fetching smart collections...", file=sys.stderr)
        smart_collections = self.fetch_collection_type("smart_collections")
        all_collections.extend(smart_collections)
        
        # Fetch custom collections  
        print("Fetching custom collections...", file=sys.stderr)
        custom_collections = self.fetch_collection_type("custom_collections")
        all_collections.extend(custom_collections)
        
        # Save in Shopify format
        output = {"collections": all_collections}
        self.save_json("collections.json", output)
        
        print(f"Saved {len(all_collections)} collections to collections.json", file=sys.stderr)
        return all_collections
    
    def fetch_collection_type(self, collection_type):
        """Fetch a specific type of collection (smart or custom)"""
        collections = []
        limit = 250
        page_info = None
        
        while True:
            if page_info:
                url = f"{self.base_url}/{collection_type}.json?limit={limit}&page_info={page_info}"
            else:
                url = f"{self.base_url}/{collection_type}.json?limit={limit}"
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching {collection_type}: {response.status_code}", file=sys.stderr)
                break
            
            data = response.json()
            collection_list = data.get(collection_type, [])
            
            if not collection_list:
                break
                
            collections.extend(collection_list)
            
            # Check for next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        page_info = link.split("page_info=")[1].split(">")[0]
                        break
            else:
                break
            
            time.sleep(0.5)
        
        return collections
    
    def fetch_collection_products(self, collection_id, collection_handle):
        """Fetch all products for a specific collection"""
        products = []
        limit = 250
        page_info = None
        
        while True:
            if page_info:
                url = f"{self.base_url}/products.json?collection_id={collection_id}&limit={limit}&page_info={page_info}"
            else:
                url = f"{self.base_url}/products.json?collection_id={collection_id}&limit={limit}"
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching products for collection {collection_handle}: {response.status_code}", file=sys.stderr)
                break
            
            data = response.json()
            product_list = data.get("products", [])
            
            if not product_list:
                break
                
            products.extend(product_list)
            
            # Check for next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        page_info = link.split("page_info=")[1].split(">")[0]
                        break
            else:
                break
            
            time.sleep(0.5)
        
        return products
    
    def create_collection_product_files(self, collections):
        """Create individual product files for each collection"""
        print("\n=== Creating Collection Product Files ===", file=sys.stderr)
        
        # Create collections directory
        os.makedirs("collections", exist_ok=True)
        
        for i, collection in enumerate(collections, 1):
            handle = collection.get("handle")
            collection_id = collection.get("id")
            title = collection.get("title")
            
            if not handle or not collection_id:
                continue
            
            print(f"[{i}/{len(collections)}] Fetching products for: {handle}", file=sys.stderr)
            
            # Fetch products for this collection
            products = self.fetch_collection_products(collection_id, handle)
            
            # Create collection products file
            output = {
                "collection": {
                    "id": collection_id,
                    "handle": handle,
                    "title": title,
                    "description": collection.get("body_html", ""),
                    "products_count": len(products)
                },
                "products": products
            }
            
            # Save to collections/[handle]_products.json
            filename = f"collections/{handle}_products.json"
            self.save_json(filename, output)
            
            print(f"  Saved {len(products)} products to {filename}", file=sys.stderr)
            
            # Rate limiting
            time.sleep(1)
    
    def fetch_blogs(self):
        """Fetch all blogs and articles"""
        print("\n=== Fetching Blogs ===", file=sys.stderr)
        
        all_blogs = []
        
        # Fetch blogs
        url = f"{self.base_url}/blogs.json"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching blogs: {response.status_code}", file=sys.stderr)
            return []
        
        blogs = response.json().get("blogs", [])
        
        # For each blog, fetch its articles
        for blog in blogs:
            blog_id = blog["id"]
            print(f"Fetching articles for blog: {blog['title']}", file=sys.stderr)
            
            # Fetch articles
            articles_url = f"{self.base_url}/blogs/{blog_id}/articles.json?limit=250"
            articles_response = requests.get(articles_url, headers=self.headers)
            
            if articles_response.status_code == 200:
                articles = articles_response.json().get("articles", [])
                blog["articles"] = articles
                print(f"  Found {len(articles)} articles", file=sys.stderr)
            else:
                blog["articles"] = []
            
            all_blogs.append(blog)
            time.sleep(0.5)
        
        # Save in Shopify format
        output = {"blogs": all_blogs}
        self.save_json("blogs.json", output)
        
        print(f"Saved {len(all_blogs)} blogs to blogs.json", file=sys.stderr)
        return all_blogs
    
    def cleanup_old_files(self):
        """Remove old format files"""
        print("\n=== Cleaning Up Old Files ===", file=sys.stderr)
        
        old_files = [
            "blog_data.json",
            "collections_with_products.json",
            "products_chunk_*.json",
            "collections_with_products_chunk_*.json",
            "*_summary.json"
        ]
        
        removed = 0
        for pattern in old_files:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                    print(f"Removed: {file}", file=sys.stderr)
                    removed += 1
                except Exception as e:
                    print(f"Error removing {file}: {e}", file=sys.stderr)
        
        if removed > 0:
            print(f"Cleaned up {removed} old files", file=sys.stderr)
    
    def create_index_files(self):
        """Create data index files"""
        print("\n=== Creating Index Files ===", file=sys.stderr)
        
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Get collection files
        collection_files = []
        if os.path.exists("collections"):
            collection_files = [f for f in os.listdir("collections") if f.endswith("_products.json")]
            collection_files.sort()
        
        # Create text index
        index_txt = f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Repository: sytrre/spacire-blog-data
Currency: GBP (British Pounds)
Format: Shopify Standard JSON

=== MAIN DATA FILES ===

Products (All Products)
URL: {base_url}products.json
Contains: Complete product catalog with descriptions, variants, images, pricing in GBP

Collections (All Collections)
URL: {base_url}collections.json
Contains: All smart and custom collections with metadata

Blogs (All Blog Posts)
URL: {base_url}blogs.json
Contains: All blogs and their articles with full content

=== COLLECTION PRODUCT FILES ===
Individual files for each collection's products:

"""
        
        for file in collection_files[:30]:  # Show first 30
            handle = file.replace("_products.json", "")
            index_txt += f"• {handle}\n  URL: {base_url}collections/{file}\n\n"
        
        if len(collection_files) > 30:
            index_txt += f"... and {len(collection_files) - 30} more collections\n"
        
        index_txt += f"""
=== USAGE ===

Get all products:
curl {base_url}products.json

Get specific collection:
curl {base_url}collections/sleep-masks_products.json

=== UPDATES ===
Auto-syncs every 30 minutes
Last updated: {timestamp}
"""
        
        with open("data_index.txt", "w") as f:
            f.write(index_txt)
        
        # Create JSON index
        index_json = {
            "generated": timestamp,
            "repository": "sytrre/spacire-blog-data",
            "base_url": base_url,
            "currency": "GBP",
            "format": "Shopify Standard JSON",
            "files": {
                "products": {
                    "url": f"{base_url}products.json",
                    "description": "All products with full data"
                },
                "collections": {
                    "url": f"{base_url}collections.json",
                    "description": "All collections"
                },
                "blogs": {
                    "url": f"{base_url}blogs.json",
                    "description": "All blogs and articles"
                },
                "collection_products": {
                    "base_url": f"{base_url}collections/",
                    "files": {}
                }
            }
        }
        
        # Add collection files to JSON index
        for file in collection_files:
            handle = file.replace("_products.json", "")
            index_json["files"]["collection_products"]["files"][handle] = {
                "url": f"{base_url}collections/{file}",
                "handle": handle
            }
        
        with open("data_index.json", "w") as f:
            json.dump(index_json, f, indent=2)
        
        print("Created data_index.txt and data_index.json", file=sys.stderr)
    
    def save_json(self, filename, data):
        """Save JSON data to file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving {filename}: {e}", file=sys.stderr)
            return False

def main():
    # Get credentials
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("ERROR: Missing SHOPIFY_SHOP_DOMAIN or SHOPIFY_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    # Initialize fetcher
    fetcher = ShopifyRESTFetcher(shop_domain, access_token)
    
    # Test connection
    if not fetcher.test_connection():
        print("Failed to connect to Shopify API", file=sys.stderr)
        sys.exit(1)
    
    # Clean up old files
    fetcher.cleanup_old_files()
    
    # Fetch all data
    products = fetcher.fetch_all_products()
    collections = fetcher.fetch_all_collections()
    
    # Create individual collection files
    if collections:
        fetcher.create_collection_product_files(collections)
    
    # Fetch blogs
    fetcher.fetch_blogs()
    
    # Create index files
    fetcher.create_index_files()
    
    print("\n✅ All done! Data fetched successfully.", file=sys.stderr)

if __name__ == "__main__":
    main()
