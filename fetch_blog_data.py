#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - Complete Version with Pagination
Fetches blog, collection, and product data from Shopify using GraphQL API
Creates separate JSON files for each data type
"""

import os
import json
import requests
from datetime import datetime
import sys

class ShopifyDataFetcher:
    def __init__(self, shop_domain, access_token):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_url = f"https://{shop_domain}/admin/api/2023-10/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
    
    def test_api_connection(self):
        """Test basic API connection"""
        query = """
        {
          shop {
            name
            myshopifyDomain
          }
        }
        """
        
        print("Testing API connection...", file=sys.stderr)
        result = self.execute_query(query, "shop_info")
        if result:
            shop_name = result.get("data", {}).get("shop", {}).get("name", "Unknown")
            print(f"Connected to shop: {shop_name}", file=sys.stderr)
            return True
        return False
    
    def fetch_and_save_blogs(self):
        """Fetch and save blog data"""
        print("Fetching blogs...", file=sys.stderr)
        
        query = """
        {
          blogs(first: 50) {
            edges {
              node {
                id
                title
                handle
                createdAt
                updatedAt
                articles(first: 250) {
                  edges {
                    node {
                      id
                      title
                      handle
                      summary
                      createdAt
                      updatedAt
                      publishedAt
                      tags
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        data = self.execute_query(query, "blogs")
        if data:
            result = self.process_blog_data(data)
            self.save_json_file("blog_data.json", result)
            return True
        return False
    
    def fetch_and_save_collections_simple(self):
        """Fetch ALL collections with pagination"""
        print("Fetching collections (simple) with pagination...", file=sys.stderr)
        
        all_collections = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            cursor_param = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            {{
              collections(first: 250{cursor_param}) {{
                edges {{
                  node {{
                    id
                    title
                    handle
                    description
                  }}
                  cursor
                }}
                pageInfo {{
                  hasNextPage
                  endCursor
                }}
              }}
            }}
            """
            
            data = self.execute_query(query, f"collections_simple_page_{len(all_collections)//250 + 1}")
            if not data:
                break
                
            collections_data = data.get("data", {}).get("collections", {})
            edges = collections_data.get("edges", [])
            
            for edge in edges:
                all_collections.append(edge["node"])
                
            page_info = collections_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            print(f"Fetched {len(edges)} collections, total so far: {len(all_collections)}", file=sys.stderr)
        
        if all_collections:
            result = self.process_collections_from_list(all_collections)
            self.save_json_file("collections.json", result)
            return True
        return False
    
    def fetch_and_save_products_simple(self):
        """Fetch ALL products with pagination"""
        print("Fetching products (simple) with pagination...", file=sys.stderr)
        
        all_products = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            cursor_param = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            {{
              products(first: 250{cursor_param}) {{
                edges {{
                  node {{
                    id
                    title
                    handle
                    description
                    productType
                    vendor
                    createdAt
                    updatedAt
                    publishedAt
                    tags
                    featuredImage {{
                      url
                      altText
                    }}
                    images(first: 10) {{
                      edges {{
                        node {{
                          url
                          altText
                        }}
                      }}
                    }}
                    variants(first: 100) {{
                      edges {{
                        node {{
                          id
                          title
                          sku
                          availableForSale
                          price
                          compareAtPrice
                          weight
                          weightUnit
                          createdAt
                          updatedAt
                          selectedOptions {{
                            name
                            value
                          }}
                        }}
                      }}
                    }}
                    options {{
                      name
                      values
                    }}
                  }}
                  cursor
                }}
                pageInfo {{
                  hasNextPage
                  endCursor
                }}
              }}
            }}
            """
            
            data = self.execute_query(query, f"products_simple_page_{len(all_products)//250 + 1}")
            if not data:
                break
                
            products_data = data.get("data", {}).get("products", {})
            edges = products_data.get("edges", [])
            
            for edge in edges:
                all_products.append(edge["node"])
                
            page_info = products_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            print(f"Fetched {len(edges)} products, total so far: {len(all_products)}", file=sys.stderr)
        
        if all_products:
            result = self.process_products_from_list(all_products)
            self.save_json_file("products.json", result)
            return True
        return False
    
    def fetch_and_save_collections_with_products(self):
        """Fetch ALL collections with their products using pagination"""
        print("Fetching collections with products using pagination...", file=sys.stderr)
        
        all_collections = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            cursor_param = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            {{
              collections(first: 100{cursor_param}) {{
                edges {{
                  node {{
                    id
                    title
                    handle
                    description
                    products(first: 100) {{
                      edges {{
                        node {{
                          id
                          title
                          handle
                          productType
                          vendor
                          createdAt
                          updatedAt
                          publishedAt
                          tags
                          featuredImage {{
                            url
                            altText
                          }}
                          variants(first: 50) {{
                            edges {{
                              node {{
                                id
                                title
                                sku
                                availableForSale
                                price
                                compareAtPrice
                                createdAt
                                updatedAt
                              }}
                            }}
                          }}
                        }}
                      }}
                    }}
                  }}
                  cursor
                }}
                pageInfo {{
                  hasNextPage
                  endCursor
                }}
              }}
            }}
            """
            
            data = self.execute_query(query, f"collections_with_products_page_{len(all_collections)//100 + 1}")
            if not data:
                break
                
            collections_data = data.get("data", {}).get("collections", {})
            edges = collections_data.get("edges", [])
            
            for edge in edges:
                all_collections.append(edge["node"])
                
            page_info = collections_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            print(f"Fetched {len(edges)} collections with products, total so far: {len(all_collections)}", file=sys.stderr)
        
        if all_collections:
            result = self.process_collections_with_products_from_list(all_collections)
            self.save_json_file("collections_with_products.json", result)
            return True
        return False
    
    def execute_query(self, query, query_type):
        """Execute GraphQL query with detailed error reporting"""
        
        try:
            print(f"Executing {query_type} query...", file=sys.stderr)
            
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json={"query": query},
                timeout=30
            )
            
            print(f"Response status for {query_type}: {response.status_code}", file=sys.stderr)
            
            if response.status_code != 200:
                print(f"HTTP Error for {query_type}: {response.status_code}", file=sys.stderr)
                print(f"Response text: {response.text[:500]}", file=sys.stderr)
                return None
            
            data = response.json()
            
            if "errors" in data:
                print(f"GraphQL errors for {query_type}:", file=sys.stderr)
                for error in data["errors"]:
                    print(f"  - {error}", file=sys.stderr)
                return None
            
            if "data" not in data:
                print(f"No data field in response for {query_type}", file=sys.stderr)
                print(f"Response: {json.dumps(data, indent=2)[:500]}", file=sys.stderr)
                return None
                
            print(f"Successfully fetched {query_type}", file=sys.stderr)
            return data
            
        except requests.exceptions.Timeout:
            print(f"Timeout error for {query_type}", file=sys.stderr)
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error for {query_type}: {str(e)}", file=sys.stderr)
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {query_type}: {str(e)}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"Unexpected error for {query_type}: {str(e)}", file=sys.stderr)
            return None
    
    def process_blog_data(self, raw_data):
        """Process blog data"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "blogs",
            "total_blogs": 0,
            "total_articles": 0,
            "blogs": []
        }
        
        blogs_data = raw_data.get("data", {}).get("blogs", {}).get("edges", [])
        processed_data["total_blogs"] = len(blogs_data)
        
        for blog_edge in blogs_data:
            blog = blog_edge["node"]
            
            blog_info = {
                "blog_id": blog["id"],
                "blog_title": blog["title"],
                "blog_handle": blog["handle"],
                "blog_created_at": blog["createdAt"],
                "blog_updated_at": blog["updatedAt"],
                "articles": []
            }
            
            articles_data = blog.get("articles", {}).get("edges", [])
            
            for article_edge in articles_data:
                article = article_edge["node"]
                
                is_published = article.get("publishedAt") is not None
                
                article_info = {
                    "article_id": article["id"],
                    "title": article["title"],
                    "handle": article["handle"],
                    "summary": article.get("summary"),
                    "status": "PUBLISHED" if is_published else "DRAFT",
                    "created_at": article["createdAt"],
                    "updated_at": article["updatedAt"],
                    "published_at": article.get("publishedAt"),
                    "tags": article.get("tags", []),
                    "is_published": is_published
                }
                
                blog_info["articles"].append(article_info)
            
            blog_info["article_count"] = len(blog_info["articles"])
            processed_data["total_articles"] += len(blog_info["articles"])
            processed_data["blogs"].append(blog_info)
        
        return processed_data
    
    def process_collections_from_list(self, collections_list):
        """Process collections from a list"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "collections",
            "total_collections": len(collections_list),
            "collections": []
        }
        
        for collection in collections_list:
            collection_info = {
                "collection_id": collection["id"],
                "title": collection["title"],
                "handle": collection["handle"],
                "description": collection.get("description")
            }
            
            processed_data["collections"].append(collection_info)
        
        return processed_data
    
    def process_products_from_list(self, products_list):
        """Process products from a list"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "products",
            "total_products": len(products_list),
            "products": []
        }
        
        for product in products_list:
            # Process images
            images = []
            images_data = product.get("images", {}).get("edges", [])
            for image_edge in images_data:
                image = image_edge["node"]
                images.append({
                    "url": image.get("url"),
                    "alt_text": image.get("altText")
                })
            
            # Process variants
            variants = []
            variants_data = product.get("variants", {}).get("edges", [])
            for variant_edge in variants_data:
                variant = variant_edge["node"]
                
                # Process selected options
                selected_options = []
                for option in variant.get("selectedOptions", []):
                    selected_options.append({
                        "name": option.get("name"),
                        "value": option.get("value")
                    })
                
                variants.append({
                    "variant_id": variant["id"],
                    "title": variant["title"],
                    "sku": variant.get("sku"),
                    "available_for_sale": variant["availableForSale"],
                    "price": variant.get("price"),
                    "compare_at_price": variant.get("compareAtPrice"),
                    "weight": variant.get("weight"),
                    "weight_unit": variant.get("weightUnit"),
                    "created_at": variant["createdAt"],
                    "updated_at": variant["updatedAt"],
                    "selected_options": selected_options
                })
            
            # Process product options
            options = []
            for option in product.get("options", []):
                options.append({
                    "name": option.get("name"),
                    "values": option.get("values", [])
                })
            
            product_info = {
                "product_id": product["id"],
                "title": product["title"],
                "handle": product["handle"],
                "description": product.get("description"),
                "product_type": product.get("productType"),
                "vendor": product.get("vendor"),
                "created_at": product["createdAt"],
                "updated_at": product["updatedAt"],
                "published_at": product.get("publishedAt"),
                "tags": product.get("tags", []),
                "featured_image": {
                    "url": product.get("featuredImage", {}).get("url"),
                    "alt_text": product.get("featuredImage", {}).get("altText")
                } if product.get("featuredImage") else None,
                "images": images,
                "images_count": len(images),
                "variants": variants,
                "variants_count": len(variants),
                "options": options
            }
            
            processed_data["products"].append(product_info)
        
        return processed_data
    
    def process_collections_with_products_from_list(self, collections_list):
        """Process collections with products from a list"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "collections_with_products",
            "total_collections": len(collections_list),
            "collections": []
        }
        
        for collection in collections_list:
            collection_info = {
                "collection_id": collection["id"],
                "title": collection["title"],
                "handle": collection["handle"],
                "description": collection.get("description"),
                "products": []
            }
            
            products_data = collection.get("products", {}).get("edges", [])
            
            for product_edge in products_data:
                product = product_edge["node"]
                
                # Process variants
                variants = []
                variants_data = product.get("variants", {}).get("edges", [])
                for variant_edge in variants_data:
                    variant = variant_edge["node"]
                    variants.append({
                        "variant_id": variant["id"],
                        "title": variant["title"],
                        "sku": variant.get("sku"),
                        "available_for_sale": variant["availableForSale"],
                        "price": variant.get("price"),
                        "compare_at_price": variant.get("compareAtPrice"),
                        "created_at": variant["createdAt"],
                        "updated_at": variant["updatedAt"]
                    })
                
                product_info = {
                    "product_id": product["id"],
                    "title": product["title"],
                    "handle": product["handle"],
                    "description": product.get("description"),
                    "product_type": product.get("productType"),
                    "vendor": product.get("vendor"),
                    "created_at": product["createdAt"],
                    "updated_at": product["updatedAt"],
                    "published_at": product.get("publishedAt"),
                    "tags": product.get("tags", []),
                    "featured_image": {
                        "url": product.get("featuredImage", {}).get("url"),
                        "alt_text": product.get("featuredImage", {}).get("altText")
                    } if product.get("featuredImage") else None,
                    "variants": variants,
                    "variants_count": len(variants)
                }
                
                collection_info["products"].append(product_info)
            
            collection_info["products_count"] = len(collection_info["products"])
            processed_data["collections"].append(collection_info)
        
        return processed_data
    
    def save_json_file(self, filename, data):
        """Save data to JSON file with chunking for large files"""
        try:
            # Save main file
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            file_size = len(json.dumps(data))
            print(f"Successfully saved {filename} with {file_size} characters", file=sys.stderr)
            
            # If file is large (over 500KB), create chunked versions
            if file_size > 500000:
                self.create_chunked_files(filename, data)
                
        except Exception as e:
            print(f"Error saving {filename}: {str(e)}", file=sys.stderr)
    
    def create_chunked_files(self, filename, data):
        """Create smaller chunked files for large datasets"""
        try:
            base_name = filename.replace('.json', '')
            data_type = data.get('data_type', 'unknown')
            
            if data_type == 'products':
                products = data.get('products', [])
                chunk_size = 50  # 50 products per chunk
                
                for i in range(0, len(products), chunk_size):
                    chunk = products[i:i + chunk_size]
                    chunk_data = {
                        "fetch_timestamp": data.get('fetch_timestamp'),
                        "shop_domain": data.get('shop_domain'),
                        "data_type": f"products_chunk_{i//chunk_size + 1}",
                        "chunk_info": {
                            "chunk_number": i//chunk_size + 1,
                            "total_chunks": (len(products) + chunk_size - 1) // chunk_size,
                            "products_in_chunk": len(chunk),
                            "total_products": len(products)
                        },
                        "products": chunk
                    }
                    
                    chunk_filename = f"{base_name}_chunk_{i//chunk_size + 1}.json"
                    with open(chunk_filename, "w", encoding="utf-8") as f:
                        json.dump(chunk_data, f, indent=2, ensure_ascii=False)
                    
                    print(f"Created chunk: {chunk_filename}", file=sys.stderr)
            
            elif data_type == 'collections_with_products':
                collections = data.get('collections', [])
                chunk_size = 20  # 20 collections per chunk
                
                for i in range(0, len(collections), chunk_size):
                    chunk = collections[i:i + chunk_size]
                    chunk_data = {
                        "fetch_timestamp": data.get('fetch_timestamp'),
                        "shop_domain": data.get('shop_domain'),
                        "data_type": f"collections_with_products_chunk_{i//chunk_size + 1}",
                        "chunk_info": {
                            "chunk_number": i//chunk_size + 1,
                            "total_chunks": (len(collections) + chunk_size - 1) // chunk_size,
                            "collections_in_chunk": len(chunk),
                            "total_collections": len(collections)
                        },
                        "collections": chunk
                    }
                    
                    chunk_filename = f"{base_name}_chunk_{i//chunk_size + 1}.json"
                    with open(chunk_filename, "w", encoding="utf-8") as f:
                        json.dump(chunk_data, f, indent=2, ensure_ascii=False)
                    
                    print(f"Created chunk: {chunk_filename}", file=sys.stderr)
            
            # Create summary file for large datasets
            summary_data = {
                "fetch_timestamp": data.get('fetch_timestamp'),
                "shop_domain": data.get('shop_domain'),
                "data_type": f"{data_type}_summary",
                "file_info": {
                    "main_file": filename,
                    "file_size_characters": len(json.dumps(data)),
                    "chunked": True,
                    "total_items": data.get('total_products') or data.get('total_collections', 0)
                }
            }
            
            # Add item summaries
            if data_type == 'products':
                summary_data["products_summary"] = [
                    {
                        "title": product.get('title'),
                        "handle": product.get('handle'),
                        "product_type": product.get('product_type'),
                        "variants_count": product.get('variants_count', 0)
                    }
                    for product in data.get('products', [])
                ]
            elif data_type == 'collections_with_products':
                summary_data["collections_summary"] = [
                    {
                        "title": collection.get('title'),
                        "handle": collection.get('handle'),
                        "products_count": collection.get('products_count', 0)
                    }
                    for collection in data.get('collections', [])
                ]
            
            summary_filename = f"{base_name}_summary.json"
            with open(summary_filename, "w", encoding="utf-8") as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            
            print(f"Created summary file: {summary_filename}", file=sys.stderr)
            
        except Exception as e:
            print(f"Error creating chunked files: {str(e)}", file=sys.stderr)
    
    def create_data_index_file(self):
        """Create a comprehensive index file with all data URLs and descriptions"""
        
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Check what files exist
        import os
        existing_files = [f for f in os.listdir('.') if f.endswith('.json')]
        
        index_content = f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Repository: sytrre/spacire-blog-data

=== COMPLETE DATA ACCESS ===

BLOG DATA:
- Main file: {base_url}blog_data.json
- Contains: All blog posts and articles (published + drafts)

COLLECTIONS LIST:
- Main file: {base_url}collections.json  
- Contains: All 94+ collections with titles, handles, descriptions
- Update frequency: Daily

PRODUCTS CATALOG:
- Main file: {base_url}products.json
- Contains: Complete product catalog with pricing, variants, images"""

        # Check for product chunks
        product_chunks = [f for f in existing_files if f.startswith('products_chunk_')]
        if product_chunks:
            product_chunks.sort()
            index_content += "\n- Chunked files (50 products each):"
            for chunk in product_chunks:
                chunk_num = chunk.replace('products_chunk_', '').replace('.json', '')
                index_content += f"\n  * {base_url}{chunk}"
            index_content += f"\n- Summary file: {base_url}products_summary.json"

        index_content += f"""

COLLECTIONS WITH PRODUCTS:
- Main file: {base_url}collections_with_products.json
- Contains: All collections with their complete product listings"""

        # Check for collections_with_products chunks  
        collection_chunks = [f for f in existing_files if f.startswith('collections_with_products_chunk_')]
        if collection_chunks:
            collection_chunks.sort()
            index_content += "\n- Chunked files (20 collections each):"
            for chunk in collection_chunks:
                chunk_num = chunk.replace('collections_with_products_chunk_', '').replace('.json', '')
                index_content += f"\n  * {base_url}{chunk}"
            index_content += f"\n- Summary file: {base_url}collections_with_products_summary.json"

        index_content += """

=== USAGE INSTRUCTIONS ===

1. ACCESS ANY FILE: Copy any URL above into browser or API call
2. MAIN FILES: Complete datasets (may be large)
3. CHUNK FILES: Smaller portions for easier processing  
4. SUMMARY FILES: Overview with titles and counts only

=== DATA STRUCTURE ===

Blog Data: Articles with titles, summaries, tags, publish status
Collections: Titles, handles, descriptions, product counts
Products: Complete details including:
  - Pricing (regular + compare-at)
  - Variants with SKUs and availability
  - Images and descriptions
  - Product options (color, size, etc.)
  - Tags, vendor, product type

=== UPDATE FREQUENCY ===

- Blogs, Products, Collections+Products: Every 30 minutes
- Collections List: Daily at 6 AM UTC
- This index file: Updates with every sync

=== TECHNICAL INFO ===

All data fetched via Shopify GraphQL API
JSON format with consistent structure
Pagination implemented (no data limits)
Automatic chunking for large files (500KB+ threshold)

Last system update: """ + timestamp

        # Save the index file
        try:
            with open("data_index.txt", "w", encoding="utf-8") as f:
                f.write(index_content)
            print("Created data index file: data_index.txt", file=sys.stderr)
            
            # Also create a JSON version for programmatic access
            json_index = {
                "generated": timestamp,
                "repository": "sytrre/spacire-blog-data", 
                "base_url": base_url,
                "files": {
                    "blogs": {
                        "main": f"{base_url}blog_data.json",
                        "description": "All blog posts and articles"
                    },
                    "collections": {
                        "main": f"{base_url}collections.json",
                        "description": "All collections list",
                        "update_frequency": "daily"
                    },
                    "products": {
                        "main": f"{base_url}products.json",
                        "description": "Complete product catalog",
                        "chunks": [f"{base_url}{chunk}" for chunk in product_chunks],
                        "summary": f"{base_url}products_summary.json" if product_chunks else None
                    },
                    "collections_with_products": {
                        "main": f"{base_url}collections_with_products.json", 
                        "description": "Collections with their products",
                        "chunks": [f"{base_url}{chunk}" for chunk in collection_chunks],
                        "summary": f"{base_url}collections_with_products_summary.json" if collection_chunks else None
                    }
                }
            }
            
            with open("data_index.json", "w", encoding="utf-8") as f:
                json.dump(json_index, f, indent=2, ensure_ascii=False)
            print("Created JSON index file: data_index.json", file=sys.stderr)
            
        except Exception as e:
            print(f"Error creating index files: {str(e)}", file=sys.stderr)

def main():
    # Get credentials from environment variables
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    sync_type = os.getenv("SYNC_TYPE", "all")
    
    if not shop_domain or not access_token:
        print("Error: Missing required environment variables", file=sys.stderr)
        print("Please set SHOPIFY_SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting data fetch for {shop_domain} (sync_type: {sync_type})", file=sys.stderr)
    
    # Initialize fetcher
    fetcher = ShopifyDataFetcher(shop_domain, access_token)
    
    # Test API connection first
    if not fetcher.test_api_connection():
        print("Failed to connect to API", file=sys.stderr)
        sys.exit(1)
    
    # Fetch data based on sync type
    success_count = 0
    total_expected = 0
    
    # Blogs
    if sync_type in ["all", "blogs", "frequent_updates"]:
        total_expected += 1
        if fetcher.fetch_and_save_blogs():
            success_count += 1
    
    # Collections list only (daily sync)
    if sync_type in ["all", "collections_list_only"]:
        total_expected += 1
        if fetcher.fetch_and_save_collections_simple():
            success_count += 1
    
    # Collections with products (frequent sync)
    if sync_type in ["all", "collections_with_products", "frequent_updates"]:
        total_expected += 1
        if fetcher.fetch_and_save_collections_with_products():
            success_count += 1
    
    # Products
    if sync_type in ["all", "products", "frequent_updates"]:
        total_expected += 1
        if fetcher.fetch_and_save_products_simple():
            success_count += 1
    
    # Create index files if we're doing a full sync or frequent updates
    if sync_type in ["all", "frequent_updates"] and success_count > 0:
        fetcher.create_data_index_file()
    
    print(f"Successfully created {success_count} out of {total_expected} data files", file=sys.stderr)
    
    if success_count == 0:
        print("No data files were created - check API permissions", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
