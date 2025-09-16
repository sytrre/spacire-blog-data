#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - REST API Version with Full Pagination
Fetches: blogs, products, collections, and individual collection products
All files are paginated at 250 items per page for optimal file sizes
Note: body_html removed from both blogs and blog articles to reduce file size
"""

import os
import json
import requests
from datetime import datetime
import sys
import time
import glob
import re

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
        self.items_per_page = 250  # Standard pagination size
        
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
        """Remove old chunk files and previous paginated files if they exist"""
        print("\n=== Cleaning Up Old Files ===", file=sys.stderr)
        
        patterns = [
            "products_chunk_*.json",
            "collections_with_products_chunk_*.json",
            "*_summary.json",
            "collections_with_products.json",
            "blog_data.json",
            # Clean up previous paginated files
            "products_page*.json",
            "blogs_page*.json",
            "collections_page*.json"
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
    
    def save_paginated_data(self, all_items, base_filename, item_type, additional_metadata=None):
        """Generic function to save paginated data"""
        total_items = len(all_items)
        total_pages = max(1, (total_items + self.items_per_page - 1) // self.items_per_page)
        
        if total_items == 0:
            # Save empty file
            output = {
                item_type: [],
                "pagination": {
                    "current_page": 1,
                    "total_pages": 0,
                    "items_per_page": self.items_per_page,
                    "total_items": 0,
                    "items_in_page": 0,
                    "has_next_page": False,
                    "has_previous_page": False
                }
            }
            if additional_metadata:
                output.update(additional_metadata)
            
            with open(f"{base_filename}.json", "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            return 1
        
        pages_created = 0
        for page_num in range(total_pages):
            start_idx = page_num * self.items_per_page
            end_idx = min(start_idx + self.items_per_page, total_items)
            page_items = all_items[start_idx:end_idx]
            
            # Determine filename
            if page_num == 0:
                filename = f"{base_filename}.json"
            else:
                filename = f"{base_filename}_page{page_num + 1}.json"
            
            # Create pagination info
            pagination = {
                "current_page": page_num + 1,
                "total_pages": total_pages,
                "items_per_page": self.items_per_page,
                "total_items": total_items,
                "items_in_page": len(page_items),
                "has_next_page": page_num < total_pages - 1,
                "has_previous_page": page_num > 0
            }
            
            # Add links to other pages
            if pagination["has_next_page"]:
                if page_num == 0:
                    pagination["next_page_file"] = f"{base_filename}_page2.json"
                else:
                    pagination["next_page_file"] = f"{base_filename}_page{page_num + 2}.json"
            
            if pagination["has_previous_page"]:
                if page_num == 1:
                    pagination["previous_page_file"] = f"{base_filename}.json"
                else:
                    pagination["previous_page_file"] = f"{base_filename}_page{page_num}.json"
            
            output = {
                item_type: page_items,
                "pagination": pagination
            }
            
            if additional_metadata:
                output.update(additional_metadata)
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            pages_created += 1
            print(f"  Saved page {page_num + 1}/{total_pages} with {len(page_items)} items to {filename}", file=sys.stderr)
        
        return pages_created
    
    def fetch_blogs_with_articles(self):
        """Fetch all blogs with their articles (without body_html) and save paginated"""
        print("\n=== FETCHING BLOGS ===", file=sys.stderr)
        
        try:
            # First get all blogs
            blogs_url = f"{self.rest_url}/blogs.json"
            response = requests.get(blogs_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching blogs: {response.status_code}", file=sys.stderr)
                # Save empty blogs file
                self.save_paginated_data([], "blogs", "blogs")
                return []
            
            blogs_data = response.json().get("blogs", [])
            all_blogs = []
            
            # For each blog, fetch all articles
            for blog in blogs_data:
                blog_id = blog["id"]
                print(f"Fetching articles for blog: {blog['title']}", file=sys.stderr)
                
                # Remove body_html from the blog object itself to reduce file size
                if "body_html" in blog:
                    del blog["body_html"]
                
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
            
            # Save blogs with pagination
            pages = self.save_paginated_data(all_blogs, "blogs", "blogs")
            print(f"✓ Saved {len(all_blogs)} blogs across {pages} page(s)", file=sys.stderr)
            return all_blogs
            
        except Exception as e:
            print(f"Error in fetch_blogs: {e}", file=sys.stderr)
            # Save empty blogs file on error
            self.save_paginated_data([], "blogs", "blogs")
            return []
    
    def fetch_all_products(self):
        """Fetch all products using REST API and save paginated"""
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
        
        # Save products with pagination
        pages = self.save_paginated_data(all_products, "products", "products")
        print(f"✓ Saved {len(all_products)} products across {pages} page(s)", file=sys.stderr)
        return all_products
    
    def fetch_all_collections(self):
        """Fetch all collections list and save paginated"""
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
        
        # Save collections with pagination
        pages = self.save_paginated_data(all_collections, "collections", "collections")
        print(f"✓ Saved {len(all_collections)} collections across {pages} page(s)", file=sys.stderr)
        return all_collections
    
    def fetch_collections_with_products_rest(self, collections):
        """Fetch products for each collection using REST API with paginated output files"""
        print("\n=== FETCHING COLLECTION PRODUCTS (REST METHOD WITH PAGINATION) ===", file=sys.stderr)
        
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
                collects_page = 0
                
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
                        
                        collects_page += 1
                        for collect in collects:
                            product_ids.append(collect["product_id"])
                        
                        print(f"    Collects page {collects_page}: found {len(collects)} product IDs (total: {len(product_ids)})", file=sys.stderr)
                        
                        # Check for next page - Fix Link header parsing
                        link_header = collects_response.headers.get("Link", "")
                        if link_header and 'rel="next"' in link_header:
                            # More robust page_info extraction
                            import re
                            match = re.search(r'page_info=([^&>]+)', link_header)
                            if match:
                                page_info = match.group(1)
                            else:
                                break
                        else:
                            break
                    else:
                        # If collects doesn't work, break to try products endpoint
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
                    page_count = 0
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
                            page_count += 1
                            print(f"    Fetched page {page_count} with {len(products)} products (total: {len(all_products)})", file=sys.stderr)
                            
                            # Check for next page - CRITICAL: Must parse Link header correctly
                            link_header = products_response.headers.get("Link", "")
                            if link_header and 'rel="next"' in link_header:
                                # Extract page_info more carefully
                                import re
                                match = re.search(r'page_info=([^&>]+)', link_header)
                                if match:
                                    page_info = match.group(1)
                                    print(f"    Found next page_info: {page_info[:30]}...", file=sys.stderr)
                                else:
                                    break
                            else:
                                print(f"    No more pages found", file=sys.stderr)
                                break
                        else:
                            print(f"    Error fetching products: {products_response.status_code}", file=sys.stderr)
                            break
                        
                        time.sleep(0.3)
                
                # Now save products in paginated files (250 per file)
                total_products = len(all_products)
                total_pages = (total_products + self.items_per_page - 1) // self.items_per_page if total_products > 0 else 1
                
                if total_products == 0:
                    # Save empty collection file
                    output = {
                        "collection": {
                            "id": collection_id,
                            "handle": handle,
                            "title": title,
                            "body_html": collection.get("body_html", ""),
                            "products_count": 0
                        },
                        "products": [],
                        "pagination": {
                            "current_page": 1,
                            "total_pages": 0,
                            "products_per_page": self.items_per_page,
                            "total_products": 0,
                            "products_in_page": 0,
                            "has_next_page": False,
                            "has_previous_page": False
                        }
                    }
                    filename = f"collections/{handle}_products.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(output, f, indent=2, ensure_ascii=False)
                else:
                    # Save products in pages of 250
                    for page_num in range(total_pages):
                        start_idx = page_num * self.items_per_page
                        end_idx = min(start_idx + self.items_per_page, total_products)
                        page_products = all_products[start_idx:end_idx]
                        
                        # Determine filename
                        if page_num == 0:
                            filename = f"collections/{handle}_products.json"
                        else:
                            filename = f"collections/{handle}_products_page{page_num + 1}.json"
                        
                        # Create pagination info
                        pagination = {
                            "current_page": page_num + 1,
                            "total_pages": total_pages,
                            "products_per_page": self.items_per_page,
                            "total_products": total_products,
                            "products_in_page": len(page_products),
                            "has_next_page": page_num < total_pages - 1,
                            "has_previous_page": page_num > 0
                        }
                        
                        # Add links to other pages
                        if pagination["has_next_page"]:
                            pagination["next_page_file"] = f"{handle}_products_page{page_num + 2}.json"
                        if pagination["has_previous_page"]:
                            if page_num == 1:
                                pagination["previous_page_file"] = f"{handle}_products.json"
                            else:
                                pagination["previous_page_file"] = f"{handle}_products_page{page_num}.json"
                        
                        output = {
                            "collection": {
                                "id": collection_id,
                                "handle": handle,
                                "title": title,
                                "body_html": collection.get("body_html", ""),
                                "products_count": total_products
                            },
                            "products": page_products,
                            "pagination": pagination
                        }
                        
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(output, f, indent=2, ensure_ascii=False)
                        
                        print(f"  ✓ Saved page {page_num + 1}/{total_pages} with {len(page_products)} products", file=sys.stderr)
                
                print(f"  ✓ Total: {total_products} products in {total_pages} page(s)", file=sys.stderr)
                successful += 1
                
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                failed.append(handle)
            
            time.sleep(0.5)
        
        print(f"\n✓ Processed {successful}/{len(collections)} collections", file=sys.stderr)
        if failed:
            print(f"✗ Failed collections: {', '.join(failed)}", file=sys.stderr)
        
        return successful
    
    def create_index(self):
        """Create index files including all paginated files"""
        print("\n=== Creating Index Files ===", file=sys.stderr)
        
        # Spacire repository URL
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Helper function to get paginated files
        def get_paginated_files(base_name):
            files = []
            if os.path.exists(f"{base_name}.json"):
                files.append(f"{base_name}.json")
            page = 2
            while os.path.exists(f"{base_name}_page{page}.json"):
                files.append(f"{base_name}_page{page}.json")
                page += 1
            return files
        
        # Get main file pages
        product_files = get_paginated_files("products")
        blog_files = get_paginated_files("blogs")
        collection_files = get_paginated_files("collections")
        
        # Get collection product files
        collection_product_data = {}
        if os.path.exists("collections"):
            all_collection_files = [f for f in os.listdir("collections") if f.endswith(".json")]
            
            for file in all_collection_files:
                # Extract collection handle
                if "_products_page" in file:
                    handle = file.split("_products_page")[0]
                elif "_products.json" in file:
                    handle = file.replace("_products.json", "")
                else:
                    continue
                
                if handle not in collection_product_data:
                    collection_product_data[handle] = []
                collection_product_data[handle].append(file)
            
            # Sort files within each collection
            for handle in collection_product_data:
                collection_product_data[handle].sort()
        
        # Create data_index.txt
        with open("data_index.txt", "w") as f:
            f.write(f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Currency: GBP
Pagination: 250 items per page

=== MAIN FILES ===

PRODUCTS ({len(product_files)} pages):
""")
            for i, file in enumerate(product_files, 1):
                f.write(f"• Page {i}: {base_url}{file}\n")
            
            f.write(f"\nCOLLECTIONS ({len(collection_files)} pages):\n")
            for i, file in enumerate(collection_files, 1):
                f.write(f"• Page {i}: {base_url}{file}\n")
            
            f.write(f"\nBLOGS ({len(blog_files)} pages):\n")
            for i, file in enumerate(blog_files, 1):
                f.write(f"• Page {i}: {base_url}{file}\n")
            
            f.write(f"\n=== COLLECTION PRODUCT FILES ({len(collection_product_data)} collections) ===\n")
            for handle in sorted(collection_product_data.keys()):
                files = collection_product_data[handle]
                if len(files) == 1:
                    f.write(f"• {handle}: {base_url}collections/{files[0]}\n")
                else:
                    f.write(f"• {handle} ({len(files)} pages):\n")
                    for file in files:
                        page_num = "Page 1" if "_page" not in file else f"Page {file.split('_page')[1].replace('.json', '')}"
                        f.write(f"  - {page_num}: {base_url}collections/{file}\n")
        
        # Create data_index.json
        index = {
            "generated": timestamp,
            "repository": "spacire-blog-data",
            "currency": "GBP",
            "pagination_size": self.items_per_page,
            "files": {
                "products": {},
                "collections": {},
                "blogs": {},
                "collection_products": {}
            }
        }
        
        # Add main files with pagination info
        if len(product_files) == 1:
            index["files"]["products"] = f"{base_url}products.json"
        else:
            index["files"]["products"] = {
                "total_pages": len(product_files),
                "files": {f"page_{i}": f"{base_url}{file}" for i, file in enumerate(product_files, 1)}
            }
        
        if len(collection_files) == 1:
            index["files"]["collections"] = f"{base_url}collections.json"
        else:
            index["files"]["collections"] = {
                "total_pages": len(collection_files),
                "files": {f"page_{i}": f"{base_url}{file}" for i, file in enumerate(collection_files, 1)}
            }
        
        if len(blog_files) == 1:
            index["files"]["blogs"] = f"{base_url}blogs.json"
        else:
            index["files"]["blogs"] = {
                "total_pages": len(blog_files),
                "files": {f"page_{i}": f"{base_url}{file}" for i, file in enumerate(blog_files, 1)}
            }
        
        # Add collection product files
        for handle in sorted(collection_product_data.keys()):
            files = collection_product_data[handle]
            if len(files) == 1:
                index["files"]["collection_products"][handle] = f"{base_url}collections/{files[0]}"
            else:
                index["files"]["collection_products"][handle] = {
                    "total_pages": len(files),
                    "files": {f"page_{i}": f"{base_url}collections/{file}" for i, file in enumerate(files, 1)}
                }
        
        with open("data_index.json", "w") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Created index with pagination info", file=sys.stderr)
        print(f"  - Products: {len(product_files)} pages", file=sys.stderr)
        print(f"  - Collections: {len(collection_files)} pages", file=sys.stderr)
        print(f"  - Blogs: {len(blog_files)} pages", file=sys.stderr)
        print(f"  - Collection Products: {len(collection_product_data)} collections", file=sys.stderr)

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
    
    # Fetch all 4 main file types with pagination
    fetcher.fetch_blogs_with_articles()
    fetcher.fetch_all_products()
    collections = fetcher.fetch_all_collections()
    
    # Fetch products for each collection with pagination
    if collections:
        fetcher.fetch_collections_with_products_rest(collections)
    
    # Create index
    fetcher.create_index()
    
    print("\n✅ All data fetched successfully with pagination!", file=sys.stderr)

if __name__ == "__main__":
    main()
