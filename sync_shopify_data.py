#!/usr/bin/env python3
"""
Spacire Shopify Data Sync - Clean Version with Full Pagination
Fetches all store data and saves with 250 items per page
"""

import os
import json
import requests
from datetime import datetime
import sys
import time
import glob

class SpacireSync:
    def __init__(self, shop_domain, access_token):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = "2023-10"
        self.rest_url = f"https://{shop_domain}/admin/api/{self.api_version}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
        self.items_per_page = 250
        
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
        """Remove old files from previous runs"""
        print("\n=== Cleaning Up Old Files ===", file=sys.stderr)
        
        patterns = [
            "products_page*.json",
            "blogs_page*.json", 
            "collections_page*.json",
            # Legacy files
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
    
    def save_paginated_data(self, all_items, base_filename, item_type):
        """Save data in pages of 250 items"""
        total_items = len(all_items)
        total_pages = max(1, (total_items + self.items_per_page - 1) // self.items_per_page)
        
        if total_items == 0:
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
            with open(f"{base_filename}.json", "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            return 1
        
        pages_created = 0
        for page_num in range(total_pages):
            start_idx = page_num * self.items_per_page
            end_idx = min(start_idx + self.items_per_page, total_items)
            page_items = all_items[start_idx:end_idx]
            
            if page_num == 0:
                filename = f"{base_filename}.json"
            else:
                filename = f"{base_filename}_page{page_num + 1}.json"
            
            pagination = {
                "current_page": page_num + 1,
                "total_pages": total_pages,
                "items_per_page": self.items_per_page,
                "total_items": total_items,
                "items_in_page": len(page_items),
                "has_next_page": page_num < total_pages - 1,
                "has_previous_page": page_num > 0
            }
            
            if pagination["has_next_page"]:
                pagination["next_page_file"] = f"{base_filename}_page{page_num + 2}.json" if page_num == 0 else f"{base_filename}_page{page_num + 2}.json"
            
            if pagination["has_previous_page"]:
                pagination["previous_page_file"] = f"{base_filename}.json" if page_num == 1 else f"{base_filename}_page{page_num}.json"
            
            output = {
                item_type: page_items,
                "pagination": pagination
            }
            
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            
            pages_created += 1
            print(f"  Saved page {page_num + 1}/{total_pages} with {len(page_items)} items to {filename}", file=sys.stderr)
        
        return pages_created
    
    def fetch_blogs_with_articles(self):
        """Fetch all blogs with their articles (body_html removed)"""
        print("\n=== FETCHING BLOGS ===", file=sys.stderr)
        
        try:
            blogs_url = f"{self.rest_url}/blogs.json"
            response = requests.get(blogs_url, headers=self.headers)
            
            if response.status_code != 200:
                print(f"Error fetching blogs: {response.status_code}", file=sys.stderr)
                self.save_paginated_data([], "blogs", "blogs")
                return []
            
            blogs_data = response.json().get("blogs", [])
            all_blogs = []
            
            for blog in blogs_data:
                blog_id = blog["id"]
                print(f"Fetching articles for blog: {blog['title']}", file=sys.stderr)
                
                # Remove body_html from blog
                if "body_html" in blog:
                    del blog["body_html"]
                
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
                        
                        # Remove body_html from articles
                        for article in articles:
                            if "body_html" in article:
                                del article["body_html"]
                        
                        all_articles.extend(articles)
                        print(f"  Fetched {len(articles)} articles", file=sys.stderr)
                        
                        link_header = articles_response.headers.get("Link", "")
                        if 'rel="next"' in link_header:
                            for link in link_header.split(","):
                                if 'rel="next"' in link:
                                    page_info = link.split("page_info=")[1].split(">")[0].split("&")[0]
                                    break
                        else:
                            break
                    else:
                        print(f"  Error fetching articles: {articles_response.status_code}", file=sys.stderr)
                        break
                    
                    time.sleep(0.5)
                
                blog["articles"] = all_articles
                all_blogs.append(blog)
                print(f"  Total articles: {len(all_articles)}", file=sys.stderr)
            
            pages = self.save_paginated_data(all_blogs, "blogs", "blogs")
            print(f"✓ Saved {len(all_blogs)} blogs across {pages} page(s)", file=sys.stderr)
            return all_blogs
            
        except Exception as e:
            print(f"Error in fetch_blogs: {e}", file=sys.stderr)
            self.save_paginated_data([], "blogs", "blogs")
            return []
    
    def fetch_all_products(self):
        """Fetch all products and save paginated"""
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
            
            link_header = response.headers.get("Link", "")
            if 'rel="next"' in link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        page_info = link.split("page_info=")[1].split(">")[0].split("&")[0]
                        break
            else:
                break
            
            time.sleep(0.5)
        
        pages = self.save_paginated_data(all_products, "products", "products")
        print(f"✓ Saved {len(all_products)} products across {pages} page(s)", file=sys.stderr)
        return all_products
    
    def fetch_all_collections(self):
        """Fetch all collections and save paginated"""
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
                
                link_header = smart_response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    for link in link_header.split(","):
                        if 'rel="next"' in link:
                            page_info = link.split("page_info=")[1].split(">")[0].split("&")[0]
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
                
                link_header = custom_response.headers.get("Link", "")
                if 'rel="next"' in link_header:
                    for link in link_header.split(","):
                        if 'rel="next"' in link:
                            page_info = link.split("page_info=")[1].split(">")[0].split("&")[0]
                            break
                else:
                    break
            else:
                break
            time.sleep(0.5)
        
        pages = self.save_paginated_data(all_collections, "collections", "collections")
        print(f"✓ Saved {len(all_collections)} collections across {pages} page(s)", file=sys.stderr)
        return all_collections
    
    def fetch_collection_products(self, collections):
        """Fetch products for each collection using REST API"""
        print("\n=== FETCHING COLLECTION PRODUCTS ===", file=sys.stderr)
        print(">>> Using REST API products endpoint <<<", file=sys.stderr)
        
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
                # Use the products endpoint with collection_id filter
                # This endpoint uses page_info for pagination
                url = f"{self.rest_url}/products.json?collection_id={collection_id}&limit=250"
                
                while url:
                    # Make request
                    response = requests.get(url, headers=self.headers)
                    
                    if response.status_code == 200:
                        products = response.json().get("products", [])
                        
                        if products:
                            all_products.extend(products)
                            print(f"    Got {len(products)} products (total: {len(all_products)})", file=sys.stderr)
                        
                        # Check for next page in Link header
                        link_header = response.headers.get("Link", "")
                        url = None  # Reset
                        
                        if link_header and 'rel="next"' in link_header:
                            # Extract next page URL from Link header
                            # Format: <https://shop.myshopify.com/admin/api/2023-10/products.json?page_info=xxx>; rel="next"
                            import re
                            match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                            if match:
                                next_url = match.group(1)
                                # Use the full URL from the Link header
                                if next_url.startswith('http'):
                                    url = next_url
                                else:
                                    # Construct full URL if relative
                                    url = f"https://{self.shop_domain}{next_url}"
                                print(f"    Found next page", file=sys.stderr)
                        
                        if not url:
                            print(f"    No more pages", file=sys.stderr)
                    else:
                        print(f"    Error {response.status_code} fetching products", file=sys.stderr)
                        if response.status_code == 400:
                            print(f"    Response: {response.text[:200]}", file=sys.stderr)
                        break
                    
                    time.sleep(0.3)
                
                # Save paginated collection products
                total_products = len(all_products)
                total_pages = max(1, (total_products + self.items_per_page - 1) // self.items_per_page)
                
                if total_products == 0:
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
                            "products_in_page": 0
                        }
                    }
                    with open(f"collections/{handle}_products.json", "w", encoding="utf-8") as f:
                        json.dump(output, f, indent=2, ensure_ascii=False)
                else:
                    for page_num in range(total_pages):
                        start_idx = page_num * self.items_per_page
                        end_idx = min(start_idx + self.items_per_page, total_products)
                        page_products = all_products[start_idx:end_idx]
                        
                        if page_num == 0:
                            filename = f"collections/{handle}_products.json"
                        else:
                            filename = f"collections/{handle}_products_page{page_num + 1}.json"
                        
                        pagination = {
                            "current_page": page_num + 1,
                            "total_pages": total_pages,
                            "products_per_page": self.items_per_page,
                            "total_products": total_products,
                            "products_in_page": len(page_products),
                            "has_next_page": page_num < total_pages - 1,
                            "has_previous_page": page_num > 0
                        }
                        
                        if pagination["has_next_page"]:
                            pagination["next_page_file"] = f"{handle}_products_page{page_num + 2}.json"
                        if pagination["has_previous_page"]:
                            pagination["previous_page_file"] = f"{handle}_products.json" if page_num == 1 else f"{handle}_products_page{page_num}.json"
                        
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
                
                print(f"  ✓ Total: {total_products} products in {total_pages} page(s)", file=sys.stderr)
                successful += 1
                
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                failed.append(handle)
            
            time.sleep(0.5)
        
        print(f"\n✓ Processed {successful}/{len(collections)} collections", file=sys.stderr)
        if failed:
            print(f"✗ Failed collections: {', '.join(failed)}", file=sys.stderr)
    
    def create_index(self):
        """Create index files"""
        print("\n=== Creating Index Files ===", file=sys.stderr)
        
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Helper to get paginated files
        def get_paginated_files(base_name):
            files = []
            if os.path.exists(f"{base_name}.json"):
                files.append(f"{base_name}.json")
            page = 2
            while os.path.exists(f"{base_name}_page{page}.json"):
                files.append(f"{base_name}_page{page}.json")
                page += 1
            return files
        
        product_files = get_paginated_files("products")
        blog_files = get_paginated_files("blogs")
        collection_files = get_paginated_files("collections")
        
        # Get collection product files
        collection_product_data = {}
        if os.path.exists("collections"):
            for file in os.listdir("collections"):
                if file.endswith(".json"):
                    if "_products_page" in file:
                        handle = file.split("_products_page")[0]
                    elif "_products.json" in file:
                        handle = file.replace("_products.json", "")
                    else:
                        continue
                    
                    if handle not in collection_product_data:
                        collection_product_data[handle] = []
                    collection_product_data[handle].append(file)
            
            for handle in collection_product_data:
                collection_product_data[handle].sort()
        
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
        
        # Add main files
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
        
        # Add collection products
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
        
        # Create simple text index
        with open("data_index.txt", "w") as f:
            f.write(f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Currency: GBP
Pagination: 250 items per page

=== MAIN FILES ===
Products: {len(product_files)} pages
Collections: {len(collection_files)} pages
Blogs: {len(blog_files)} pages

=== COLLECTION PRODUCTS ===
Total Collections: {len(collection_product_data)}
""")
        
        print(f"✓ Created index files", file=sys.stderr)
        print(f"  - Products: {len(product_files)} pages", file=sys.stderr)
        print(f"  - Collections: {len(collection_files)} pages", file=sys.stderr)
        print(f"  - Blogs: {len(blog_files)} pages", file=sys.stderr)
        print(f"  - Collection Products: {len(collection_product_data)} collections", file=sys.stderr)

def main():
    print("\n=== SPACIRE SHOPIFY SYNC - CLEAN VERSION ===", file=sys.stderr)
    print(">>> If you see this message, the new script is running! <<<\n", file=sys.stderr)
    
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("ERROR: Missing credentials", file=sys.stderr)
        sys.exit(1)
    
    syncer = SpacireSync(shop_domain, access_token)
    
    if not syncer.test_connection():
        sys.exit(1)
    
    syncer.cleanup_old_files()
    syncer.fetch_blogs_with_articles()
    syncer.fetch_all_products()
    collections = syncer.fetch_all_collections()
    
    if collections:
        syncer.fetch_collection_products(collections)
    
    syncer.create_index()
    
    print("\n✅ All data synced successfully!", file=sys.stderr)

if __name__ == "__main__":
    main()
