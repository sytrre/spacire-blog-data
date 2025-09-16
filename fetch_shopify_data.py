#!/usr/bin/env python3
"""
Spacire Data Fetcher - Focused on Blogs and Smart Collections
Priority: Blogs and Individual Collection Products
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
        self.graphql_url = f"https://{shop_domain}/admin/api/{self.api_version}/graphql.json"
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
        """Remove old chunk files"""
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
        """Fetch all blogs with their articles - PRIORITY ITEM"""
        print("\n=== FETCHING BLOGS (Priority) ===", file=sys.stderr)
        
        # First get all blogs
        blogs_url = f"{self.rest_url}/blogs.json"
        response = requests.get(blogs_url, headers=self.headers)
        
        if response.status_code != 200:
            print(f"Error fetching blogs: {response.status_code}", file=sys.stderr)
            return []
        
        blogs_data = response.json().get("blogs", [])
        all_blogs = []
        
        # For each blog, fetch all articles
        for blog in blogs_data:
            blog_id = blog["id"]
            print(f"Fetching articles for blog: {blog['title']}", file=sys.stderr)
            
            # Fetch all articles for this blog with pagination
            all_articles = []
            page = 1
            
            while True:
                articles_url = f"{self.rest_url}/blogs/{blog_id}/articles.json?limit=250&page={page}"
                articles_response = requests.get(articles_url, headers=self.headers)
                
                if articles_response.status_code == 200:
                    articles = articles_response.json().get("articles", [])
                    if not articles:
                        break
                    all_articles.extend(articles)
                    print(f"  Fetched {len(articles)} articles (page {page})", file=sys.stderr)
                    page += 1
                    
                    # If we got less than 250, we're done
                    if len(articles) < 250:
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
        smart_url = f"{self.rest_url}/smart_collections.json?limit=250"
        smart_response = requests.get(smart_url, headers=self.headers)
        if smart_response.status_code == 200:
            all_collections.extend(smart_response.json().get("smart_collections", []))
        
        # Fetch custom collections
        custom_url = f"{self.rest_url}/custom_collections.json?limit=250"
        custom_response = requests.get(custom_url, headers=self.headers)
        if custom_response.status_code == 200:
            all_collections.extend(custom_response.json().get("custom_collections", []))
        
        # Save collections.json
        output = {"collections": all_collections}
        with open("collections.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Saved {len(all_collections)} collections to collections.json", file=sys.stderr)
        return all_collections
    
    def fetch_collections_with_products(self, collections):
        """Fetch products for each collection using GraphQL - PRIORITY ITEM"""
        print("\n=== FETCHING COLLECTION PRODUCTS (Priority) ===", file=sys.stderr)
        
        os.makedirs("collections", exist_ok=True)
        
        successful = 0
        failed = []
        
        for i, collection in enumerate(collections, 1):
            handle = collection.get("handle")
            title = collection.get("title")
            collection_id = collection.get("id")
            
            if not handle:
                continue
            
            print(f"[{i}/{len(collections)}] Fetching products for: {handle}", file=sys.stderr)
            
            # Fetch products using GraphQL
            all_products = []
            has_next = True
            cursor = None
            
            while has_next:
                after_clause = f', after: "{cursor}"' if cursor else ''
                
                query = f"""
                query {{
                  collectionByHandle(handle: "{handle}") {{
                    id
                    title
                    handle
                    descriptionHtml
                    products(first: 250{after_clause}) {{
                      edges {{
                        node {{
                          id
                          title
                          handle
                          vendor
                          productType
                          descriptionHtml
                          createdAt
                          updatedAt
                          publishedAt
                          tags
                          images(first: 20) {{
                            edges {{
                              node {{
                                id
                                url
                                altText
                                width
                                height
                              }}
                            }}
                          }}
                          variants(first: 100) {{
                            edges {{
                              node {{
                                id
                                title
                                price
                                compareAtPrice
                                sku
                                position
                                inventoryQuantity
                                barcode
                                weight
                                createdAt
                                updatedAt
                              }}
                            }}
                          }}
                          options {{
                            id
                            name
                            position
                            values
                          }}
                        }}
                        cursor
                      }}
                      pageInfo {{
                        hasNextPage
                      }}
                    }}
                  }}
                }}
                """
                
                response = requests.post(
                    self.graphql_url,
                    headers=self.headers,
                    json={"query": query}
                )
                
                if response.status_code != 200:
                    print(f"  ✗ Error {response.status_code}", file=sys.stderr)
                    failed.append(handle)
                    break
                
                data = response.json()
                
                if "errors" in data:
                    print(f"  ✗ GraphQL error", file=sys.stderr)
                    failed.append(handle)
                    break
                
                collection_data = data.get("data", {}).get("collectionByHandle")
                
                if not collection_data:
                    print(f"  ✗ Not found", file=sys.stderr)
                    failed.append(handle)
                    break
                
                products_connection = collection_data.get("products", {})
                edges = products_connection.get("edges", [])
                
                for edge in edges:
                    product = self.format_product(edge["node"])
                    all_products.append(product)
                    cursor = edge.get("cursor")
                
                page_info = products_connection.get("pageInfo", {})
                has_next = page_info.get("hasNextPage", False)
                
                if not has_next or not edges:
                    break
                
                time.sleep(0.3)
            
            # Save collection file
            if collection_data or len(all_products) == 0:
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
                
                print(f"  ✓ Saved {len(all_products)} products", file=sys.stderr)
                successful += 1
            
            time.sleep(0.5)
        
        print(f"\n✓ Processed {successful}/{len(collections)} collections", file=sys.stderr)
        if failed:
            print(f"✗ Failed: {len(failed)}", file=sys.stderr)
        
        return successful
    
    def format_product(self, node):
        """Convert GraphQL product to REST format"""
        gid = node.get("id", "")
        product_id = gid.split("/")[-1] if "/" in gid else gid
        
        # Format images
        images = []
        for img_edge in node.get("images", {}).get("edges", []):
            img = img_edge["node"]
            img_id = img.get("id", "").split("/")[-1] if "/" in img.get("id") else ""
            images.append({
                "id": int(img_id) if img_id.isdigit() else img_id,
                "src": img.get("url"),
                "alt": img.get("altText")
            })
        
        # Format variants
        variants = []
        for var_edge in node.get("variants", {}).get("edges", []):
            var = var_edge["node"]
            var_id = var.get("id", "").split("/")[-1] if "/" in var.get("id") else ""
            variants.append({
                "id": int(var_id) if var_id.isdigit() else var_id,
                "product_id": int(product_id) if product_id.isdigit() else product_id,
                "title": var.get("title"),
                "price": var.get("price"),
                "compare_at_price": var.get("compareAtPrice"),
                "sku": var.get("sku"),
                "position": var.get("position", 1),
                "inventory_quantity": var.get("inventoryQuantity"),
                "barcode": var.get("barcode"),
                "weight": var.get("weight"),
                "weight_unit": "kg"
            })
        
        # Format options
        options = []
        for opt in node.get("options", []):
            options.append({
                "name": opt.get("name"),
                "position": opt.get("position", 1),
                "values": opt.get("values", [])
            })
        
        return {
            "id": int(product_id) if product_id.isdigit() else product_id,
            "title": node.get("title"),
            "body_html": node.get("descriptionHtml", ""),
            "vendor": node.get("vendor"),
            "product_type": node.get("productType"),
            "created_at": node.get("createdAt"),
            "handle": node.get("handle"),
            "updated_at": node.get("updatedAt"),
            "published_at": node.get("publishedAt"),
            "tags": ", ".join(node.get("tags", [])) if isinstance(node.get("tags"), list) else node.get("tags", ""),
            "images": images,
            "variants": variants,
            "options": options
        }
    
    def create_index(self):
        """Create index files"""
        print("\n=== Creating Index Files ===", file=sys.stderr)
        
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

=== MAIN FILES ===
• Blogs: {base_url}blogs.json
• Products: {base_url}products.json
• Collections: {base_url}collections.json

=== ALL COLLECTION FILES ===
""")
            for file in collection_files:
                handle = file.replace("_products.json", "")
                f.write(f"• {handle}: {base_url}collections/{file}\n")
        
        # Create data_index.json
        index = {
            "generated": timestamp,
            "repository": "sytrre/spacire-blog-data",
            "currency": "GBP",
            "files": {
                "blogs": f"{base_url}blogs.json",
                "products": f"{base_url}products.json",
                "collections": f"{base_url}collections.json",
                "collection_products": {}
            }
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
    
    # Fetch priority items
    fetcher.fetch_blogs_with_articles()
    
    # Fetch all collections and products
    collections = fetcher.fetch_all_collections()
    fetcher.fetch_all_products()
    
    # Fetch products for each collection
    if collections:
        fetcher.fetch_collections_with_products(collections)
    
    # Create index
    fetcher.create_index()
    
    print("\n✅ All data fetched successfully!", file=sys.stderr)

if __name__ == "__main__":
    main()
