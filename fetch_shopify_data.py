#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - Complete version with REST API
Fetches: blogs, products, collections, and individual collection products
Note: body_html removed from blog articles to reduce file size
"""

import os
import json
import requests
from datetime import datetime
import sys
import time
import glob

class SpacireDataFetcher:
    def __init__(self, shop_domain, access_token):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = "2023-10"
        self.rest_url = f"https://{shop_domain}/admin/api/{self.api_version}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        
    def test_connection(self):
        """Test API connection"""
        print("Testing API connection...", file=sys.stderr)
        
        url = f"{self.rest_url}/shop.json"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 200:
            shop_data = response.json()["shop"]
            print(f"✓ Connected to: {shop_data['name']}", file=sys.stderr)
            print(f"✓ Currency: {shop_data['currency']}", file=sys.stderr)
            return True
        else:
            print(f"✗ Failed to connect. Status: {response.status_code}", file=sys.stderr)
            return False
    
    def cleanup_old_files(self):
        """Remove old chunk files if they exist"""
        print("\n=== Cleaning Up Old Files ===", file=sys.stderr)
        
        patterns = [
            "products_chunk_*.json",
            "collections_with_products_chunk_*.json",
            "*_summary.json",
            "collections_with_products.json",
            "blog_data.json"
        ]
        
        removed = 0
        for pattern in patterns:
            for file in glob.glob(pattern):
                try:
                    os.remove(file)
                    print(f"  Removed: {file}", file=sys.stderr)
                    removed += 1
                except:
                    pass
        
        if removed > 0:
            print(f"  Cleaned up {removed} old files", file=sys.stderr)
    
    def fetch_blogs_with_articles(self):
        """Fetch all blogs with their articles (without body_html)"""
        print("\n=== FETCHING BLOGS ===", file=sys.stderr)
        
        try:
            # First get all blogs
            blogs_url = f"{self.rest_url}/blogs.json"
            response = requests.get(blogs_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching blogs: {response.status_code}", file=sys.stderr)
                # Save empty blogs file
                with open("blogs.json", "w") as f:
                    json.dump({"blogs": []}, f, indent=2)
                return []
            
            blogs_data = response.json().get("blogs", [])
            all_blogs = []
            
            # For each blog, fetch all articles
            for blog in blogs_data:
                blog_id = blog["id"]
                print(f"Fetching articles for blog: {blog['title']}", file=sys.stderr)
                
                # Fetch all articles with pagination
                all_articles = []
                page_info = None
                
                while True:
                    if page_info:
                        articles_url = f"{self.rest_url}/blogs/{blog_id}/articles.json?limit=250&page_info={page_info}"
                    else:
                        articles_url = f"{self.rest_url}/blogs/{blog_id}/articles.json?limit=250"
                    
                    articles_response = requests.get(articles_url, headers=self.headers)
                    
                    if articles_response.status_code == 200:
                        articles = articles_response.json().get("articles", [])
                        if not articles:
                            break
                        
                        # Remove body_html field from each article to reduce file size
                        for article in articles:
                            if "body_html" in article:
                                del article["body_html"]
                        
                        all_articles.extend(articles)
                        print(f"  Fetched {len(articles)} articles", file=sys.stderr)
                        
                        # Check for next page
                        link_header = articles_response.headers.get("Link", "")
                        if 'rel="next"' in link_header:
                            for link in link_header.split(","):
                                if 'rel="next"' in link:
                                    page_info = link.split("page_info=")[1].split(">")[0]
                                    break
                        else:
                            break
                    else:
                        print(f"  Error fetching articles: {articles_response.status_code}", file=sys.stderr)
                        break
                    
                    time.sleep(0.5)
                
                # Add articles to blog
                blog["articles"] = all_articles
                all_blogs.append(blog)
                
                print(f"  Total articles: {len(all_articles)}", file=sys.stderr)
            
            # Save blogs.json
            output = {"blogs": all_blogs}
            with open("blogs.json", "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            print(f"✓ Saved {len(all_blogs)} blogs to blogs.json", file=sys.stderr)
            return all_blogs
            
        except Exception as e:
            print(f"Error in fetch_blogs: {e}", file=sys.stderr)
            # Save empty blogs file on error
            with open("blogs.json", "w") as f:
                json.dump({"blogs": []}, f, indent=2)
            return []
    
    def fetch_all_products(self):
        """Fetch all products using REST API"""
        print("\n=== FETCHING ALL PRODUCTS ===", file=sys.stderr)
        
        all_products = []
        limit = 250
        page_info = None
        
        while True:
            if page_info:
                url = f"{self.rest_url}/products.json?limit={limit}&page_info={page_info}"
            else:
                url = f"{self.rest_url}/products.json?limit={limit}"
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching products: {response.status_code}", file=sys.stderr)
                break
            
            data = response.json()
            products = data.get("products", [])
            
            if not products:
                break
                
            all_products.extend(products)
            print(f"  Fetched {len(products)} products. Total: {len(all_products)}", file=sys.stderr)
            
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
        
        # Save products.json
        output = {"products": all_products}
        with open("products.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Saved {len(all_products)} products to products.json", file=sys.stderr)
        return all_products
    
    def fetch_all_collections(self):
        """Fetch all collections list"""
        print("\n=== FETCHING ALL COLLECTIONS ===", file=sys.stderr)
        
        all_collections = []
        
        # Fetch smart collections
        page_info = None
        while True:
            if page_info:
                smart_url = f"{self.rest_url}/smart_collections.json?limit=250&page_info={page_info}"
            else:
                smart_url = f"{self.rest_url}/smart_collections.json?limit=250"
            
            smart_response = requests.get(smart_url, headers=self.headers)
            if smart_response.status_code == 200:
                smart_cols = smart_response.json().get("smart_collections", [])
                if not smart_cols:
                    break
                all_collections.extend(smart_cols)
                
                # Check for next page
                link_header = smart_response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    for link in link_header.split(","):
                        if 'rel="next"' in link:
                            page_info = link.split("page_info=")[1].split(">")[0]
                            break
                else:
                    break
            else:
                break
            time.sleep(0.5)
        
        # Fetch custom collections
        page_info = None
        while True:
            if page_info:
                custom_url = f"{self.rest_url}/custom_collections.json?limit=250&page_info={page_info}"
            else:
                custom_url = f"{self.rest_url}/custom_collections.json?limit=250"
            
            custom_response = requests.get(custom_url, headers=self.headers)
            if custom_response.status_code == 200:
                custom_cols = custom_response.json().get("custom_collections", [])
                if not custom_cols:
                    break
                all_collections.extend(custom_cols)
                
                # Check for next page
                link_header = custom_response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    for link in link_header.split(","):
                        if 'rel="next"' in link:
                            page_info = link.split("page_info=")[1].split(">")[0]
                            break
                else:
                    break
            else:
                break
            time.sleep(0.5)
        
        # Save collections.json
        output = {"collections": all_collections}
        with open("collections.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Saved {len(all_collections)} collections to collections.json", file=sys.stderr)
        return all_collections
    
    def fetch_collections_with_products_rest(self, collections):
        """Fetch products for each collection using REST API with collects endpoint"""
        print("\n=== FETCHING COLLECTION PRODUCTS (REST METHOD) ===", file=sys.stderr)
        
        os.makedirs("collections", exist_ok=True)
        
        successful = 0
        failed = []
        
        for i, collection in enumerate(collections, 1):
            handle = collection.get("handle")
            title = collection.get("title")
            collection_id = collection.get("id")
            
            if not handle or not collection_id:
                continue
            
            print(f"[{i}/{len(collections)}] Fetching products for: {handle}", file=sys.stderr)
            
            all_products = []
            
            try:
                # Use the /collects endpoint to get product IDs for this collection
                page_info = None
                product_ids = []
                
                while True:
                    if page_info:
                        collects_url = f"{self.rest_url}/collects.json?collection_id={collection_id}&limit=250&page_info={page_info}"
                    else:
                        collects_url = f"{self.rest_url}/collects.json?collection_id={collection_id}&limit=250"
                    
                    collects_response = requests.get(collects_url, headers=self.headers)
                    
                    if collects_response.status_code == 200:
                        collects = collects_response.json().get("collects", [])
                        if not collects:
                            break
                        
                        for collect in collects:
                            product_ids.append(collect["product_id"])
                        
                        # Check for next page
                        link_header = collects_response.headers.get("Link", "")
                        if 'rel="next"' in link_header:
                            for link in link_header.split(","):
                                if 'rel="next"' in link:
                                    page_info = link.split("page_info=")[1].split(">")[0]
                                    break
                        else:
                            break
                    else:
                        # If collects doesn't work, try products with collection_id
                        break
                    
                    time.sleep(0.3)
                
                # If we got product IDs from collects, fetch the actual products
                if product_ids:
                    # Batch fetch products (Shopify allows up to 250 IDs per request)
                    for batch_start in range(0, len(product_ids), 250):
                        batch_ids = product_ids[batch_start:batch_start + 250]
                        ids_string = ",".join(str(pid) for pid in batch_ids)
                        
                        products_url = f"{self.rest_url}/products.json?ids={ids_string}&limit=250"
                        products_response = requests.get(products_url, headers=self.headers)
                        
                        if products_response.status_code == 200:
                            products = products_response.json().get("products", [])
                            all_products.extend(products)
                        
                        time.sleep(0.3)
                
                # If collects didn't work or returned nothing, try direct products endpoint
                if not all_products:
                    page_info = None
                    while True:
                        if page_info:
                            products_url = f"{self.rest_url}/products.json?collection_id={collection_id}&limit=250&page_info={page_info}"
                        else:
                            products_url = f"{self.rest_url}/products.json?collection_id={collection_id}&limit=250"
                        
                        products_response = requests.get(products_url, headers=self.headers)
                        
                        if products_response.status_code == 200:
                            products = products_response.json().get("products", [])
                            if not products:
                                break
                            
                            all_products.extend(products)
                            
                            # Check for next page
                            link_header = products_response.headers.get("Link", "")
                            if 'rel="next"' in link_header:
                                for link in link_header.split(","):
                                    if 'rel="next"' in link:
                                        page_info = link.split("page_info=")[1].split(">")[0]
                                        break
                            else:
                                break
                        else:
                            break
                        
                        time.sleep(0.3)
                
                print(f"  ✓ Found {len(all_products)} products", file=sys.stderr)
                successful += 1
                
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                failed.append(handle)
            
            # Save collection file regardless (even if empty)
            output = {
                "collection": {
                    "id": collection_id,
                    "handle": handle,
                    "title": title,
                    "body_html": collection.get("body_html", ""),
                    "products_count": len(all_products)
                },
                "products": all_products
            }
            
            filename = f"collections/{handle}_products.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            time.sleep(0.5)
        
        print(f"\n✓ Processed {successful}/{len(collections)} collections", file=sys.stderr)
        if failed:
            print(f"✗ Failed collections: {', '.join(failed)}", file=sys.stderr)
        
        return successful
    
    def create_index(self):
        """Create index files"""
        print("\n=== Creating Index Files ===", file=sys.stderr)
        
        # Spacire repository URL
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Count collection files
        collection_files = []
        if os.path.exists("collections"):
            collection_files = [f for f in os.listdir("collections") if f.endswith("_products.json")]
            collection_files.sort()
        
        # Create data_index.txt with ALL collections
        with open("data_index.txt", "w") as f:
            f.write(f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Currency: GBP
Total Collections: {len(collection_files)}

=== MAIN FILES (All 4 Working) ===
• Blogs: {base_url}blogs.json
• Products: {base_url}products.json
• Collections: {base_url}collections.json

=== ALL COLLECTION PRODUCT FILES ===
""")
            for file in collection_files:
                handle = file.replace("_products.json", "")
                f.write(f"• {handle}: {base_url}collections/{file}\n")
        
        # Create data_index.json
        index = {
            "generated": timestamp,
            "repository": "spacire-blog-data",
            "currency": "GBP",
            "files": {
                "blogs": f"{base_url}blogs.json",
                "products": f"{base_url}products.json",
                "collections": f"{base_url}collections.json",
                "collection_products": {}
            },
            "total_collections": len(collection_files)
        }
        
        for file in collection_files:
            handle = file.replace("_products.json", "")
            index["files"]["collection_products"][handle] = f"{base_url}collections/{file}"
        
        with open("data_index.json", "w") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Created index with {len(collection_files)} collections", file=sys.stderr)

def main():
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("ERROR: Missing credentials", file=sys.stderr)
        sys.exit(1)
    
    fetcher = SpacireDataFetcher(shop_domain, access_token)
    
    if not fetcher.test_connection():
        sys.exit(1)
    
    # Clean up old files
    fetcher.cleanup_old_files()
    
    # Fetch all 4 main file types
    fetcher.fetch_blogs_with_articles()
    fetcher.fetch_all_products()
    collections = fetcher.fetch_all_collections()
    
    # Fetch products for each collection
    if collections:
        fetcher.fetch_collections_with_products_rest(collections)
    
    # Create index
    fetcher.create_index()
    
    print("\n✅ All data fetched successfully!", file=sys.stderr)

if __name__ == "__main__":
    main()
