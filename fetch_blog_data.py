#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - Enhanced Version
Fetches complete product data matching Shopify's JSON structure
Creates both main files and individual collection files
"""

import os
import json
import requests
from datetime import datetime
import sys
import glob

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
            currencyCode
          }
        }
        """
        
        print("Testing API connection...", file=sys.stderr)
        result = self.execute_query(query, "shop_info")
        if result:
            shop_name = result.get("data", {}).get("shop", {}).get("name", "Unknown")
            currency = result.get("data", {}).get("shop", {}).get("currencyCode", "GBP")
            print(f"Connected to shop: {shop_name} (Currency: {currency})", file=sys.stderr)
            return True
        return False
    
    def fetch_all_products(self):
        """Fetch ALL products with complete data matching Shopify JSON structure"""
        print("Fetching all products with full data...", file=sys.stderr)
        
        all_products = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            cursor_param = f', after: "{cursor}"' if cursor else ""
            
            # Enhanced query to get ALL product data including HTML descriptions
            query = f"""
            {{
              products(first: 250{cursor_param}) {{
                edges {{
                  node {{
                    id
                    title
                    handle
                    descriptionHtml
                    vendor
                    productType
                    createdAt
                    updatedAt
                    publishedAt
                    tags
                    status
                    totalInventory
                    onlineStoreUrl
                    seo {{
                      title
                      description
                    }}
                    featuredImage {{
                      url
                      altText
                      width
                      height
                    }}
                    images(first: 20) {{
                      edges {{
                        node {{
                          url
                          altText
                          width
                          height
                          id
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
                          availableForSale
                          barcode
                          weight
                          weightUnit
                          taxable
                          requiresShipping
                          createdAt
                          updatedAt
                          image {{
                            url
                            altText
                            width
                            height
                          }}
                          selectedOptions {{
                            name
                            value
                          }}
                        }}
                      }}
                    }}
                    options {{
                      id
                      name
                      position
                      values
                    }}
                    priceRange {{
                      minVariantPrice {{
                        amount
                        currencyCode
                      }}
                      maxVariantPrice {{
                        amount
                        currencyCode
                      }}
                    }}
                    compareAtPriceRange {{
                      minVariantPrice {{
                        amount
                        currencyCode
                      }}
                      maxVariantPrice {{
                        amount
                        currencyCode
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
            
            data = self.execute_query(query, f"products_page_{len(all_products)//250 + 1}")
            if not data:
                break
                
            products_data = data.get("data", {}).get("products", {})
            edges = products_data.get("edges", [])
            
            for edge in edges:
                product = self.format_product_for_json(edge["node"])
                all_products.append(product)
                
            page_info = products_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            print(f"Fetched {len(edges)} products, total so far: {len(all_products)}", file=sys.stderr)
        
        # Create Shopify-style products.json
        products_output = {
            "products": all_products
        }
        
        self.save_json_file("products.json", products_output)
        print(f"Saved {len(all_products)} products to products.json", file=sys.stderr)
        
        return all_products
    
    def fetch_all_collections(self):
        """Fetch ALL collections matching Shopify JSON structure"""
        print("Fetching all collections...", file=sys.stderr)
        
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
                    descriptionHtml
                    updatedAt
                    publishedOnCurrentPublication
                    sortOrder
                    templateSuffix
                    productsCount
                    image {{
                      url
                      altText
                      width
                      height
                    }}
                    seo {{
                      title
                      description
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
            
            data = self.execute_query(query, f"collections_page_{len(all_collections)//250 + 1}")
            if not data:
                break
                
            collections_data = data.get("data", {}).get("collections", {})
            edges = collections_data.get("edges", [])
            
            for edge in edges:
                collection = self.format_collection_for_json(edge["node"])
                all_collections.append(collection)
                
            page_info = collections_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            print(f"Fetched {len(edges)} collections, total so far: {len(all_collections)}", file=sys.stderr)
        
        # Create Shopify-style collections.json
        collections_output = {
            "collections": all_collections
        }
        
        self.save_json_file("collections.json", collections_output)
        print(f"Saved {len(all_collections)} collections to collections.json", file=sys.stderr)
        
        return all_collections
    
    def fetch_collection_products(self, collection_handle):
        """Fetch products for a specific collection"""
        print(f"Fetching products for collection: {collection_handle}", file=sys.stderr)
        
        all_products = []
        has_next_page = True
        cursor = None
        
        while has_next_page:
            cursor_param = f', after: "{cursor}"' if cursor else ""
            
            query = f"""
            {{
              collectionByHandle(handle: "{collection_handle}") {{
                id
                title
                handle
                products(first: 250{cursor_param}) {{
                  edges {{
                    node {{
                      id
                      title
                      handle
                      descriptionHtml
                      vendor
                      productType
                      createdAt
                      updatedAt
                      publishedAt
                      tags
                      status
                      totalInventory
                      onlineStoreUrl
                      seo {{
                        title
                        description
                      }}
                      featuredImage {{
                        url
                        altText
                        width
                        height
                      }}
                      images(first: 20) {{
                        edges {{
                          node {{
                            url
                            altText
                            width
                            height
                            id
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
                            availableForSale
                            barcode
                            weight
                            weightUnit
                            taxable
                            requiresShipping
                            createdAt
                            updatedAt
                            image {{
                              url
                              altText
                              width
                              height
                            }}
                            selectedOptions {{
                              name
                              value
                            }}
                          }}
                        }}
                      }}
                      options {{
                        id
                        name
                        position
                        values
                      }}
                      priceRange {{
                        minVariantPrice {{
                          amount
                          currencyCode
                        }}
                        maxVariantPrice {{
                          amount
                          currencyCode
                        }}
                      }}
                      compareAtPriceRange {{
                        minVariantPrice {{
                          amount
                          currencyCode
                        }}
                        maxVariantPrice {{
                          amount
                          currencyCode
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
            }}
            """
            
            data = self.execute_query(query, f"collection_{collection_handle}_products")
            if not data or not data.get("data", {}).get("collectionByHandle"):
                break
                
            collection_data = data.get("data", {}).get("collectionByHandle", {})
            products_data = collection_data.get("products", {})
            edges = products_data.get("edges", [])
            
            for edge in edges:
                product = self.format_product_for_json(edge["node"])
                all_products.append(product)
                
            page_info = products_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
        
        return all_products
    
    def format_product_for_json(self, product_node):
        """Format product data to match Shopify's JSON structure"""
        # Extract Shopify numeric ID from GraphQL ID
        gid = product_node.get("id", "")
        numeric_id = gid.split("/")[-1] if "/" in gid else gid
        
        # Format images
        images = []
        for img_edge in product_node.get("images", {}).get("edges", []):
            img = img_edge["node"]
            images.append({
                "id": img.get("id", "").split("/")[-1] if "/" in img.get("id", "") else img.get("id"),
                "position": len(images) + 1,
                "created_at": product_node.get("createdAt"),
                "updated_at": product_node.get("updatedAt"),
                "alt": img.get("altText"),
                "width": img.get("width"),
                "height": img.get("height"),
                "src": img.get("url"),
                "variant_ids": []
            })
        
        # Format variants
        variants = []
        for var_edge in product_node.get("variants", {}).get("edges", []):
            var = var_edge["node"]
            var_gid = var.get("id", "")
            var_numeric_id = var_gid.split("/")[-1] if "/" in var_gid else var_gid
            
            # Format options for variant
            options = []
            for opt in var.get("selectedOptions", []):
                options.append(opt.get("value"))
            
            variants.append({
                "id": var_numeric_id,
                "product_id": numeric_id,
                "title": var.get("title"),
                "price": var.get("price"),
                "compare_at_price": var.get("compareAtPrice"),
                "sku": var.get("sku"),
                "position": var.get("position", 1),
                "inventory_quantity": var.get("inventoryQuantity"),
                "available": var.get("availableForSale", True),
                "barcode": var.get("barcode"),
                "grams": int(float(var.get("weight", 0)) * 1000) if var.get("weight") else 0,
                "weight": var.get("weight", 0),
                "weight_unit": var.get("weightUnit", "kg"),
                "taxable": var.get("taxable", True),
                "requires_shipping": var.get("requiresShipping", True),
                "created_at": var.get("createdAt"),
                "updated_at": var.get("updatedAt"),
                "featured_image": {
                    "id": var.get("image", {}).get("id", "").split("/")[-1] if var.get("image", {}).get("id") else None,
                    "position": 1,
                    "src": var.get("image", {}).get("url"),
                    "alt": var.get("image", {}).get("altText"),
                    "width": var.get("image", {}).get("width"),
                    "height": var.get("image", {}).get("height")
                } if var.get("image") else None,
                "option1": options[0] if len(options) > 0 else None,
                "option2": options[1] if len(options) > 1 else None,
                "option3": options[2] if len(options) > 2 else None
            })
        
        # Format options
        options = []
        for opt in product_node.get("options", []):
            opt_id = opt.get("id", "").split("/")[-1] if "/" in opt.get("id", "") else opt.get("id")
            options.append({
                "id": opt_id,
                "product_id": numeric_id,
                "name": opt.get("name"),
                "position": opt.get("position", len(options) + 1),
                "values": opt.get("values", [])
            })
        
        # Build the product object
        product = {
            "id": numeric_id,
            "title": product_node.get("title"),
            "body_html": product_node.get("descriptionHtml", ""),
            "vendor": product_node.get("vendor"),
            "product_type": product_node.get("productType"),
            "created_at": product_node.get("createdAt"),
            "handle": product_node.get("handle"),
            "updated_at": product_node.get("updatedAt"),
            "published_at": product_node.get("publishedAt"),
            "template_suffix": None,
            "published_scope": "web",
            "tags": ", ".join(product_node.get("tags", [])) if product_node.get("tags") else "",
            "status": product_node.get("status", "active").lower(),
            "variants": variants,
            "options": options,
            "images": images,
            "image": {
                "id": images[0]["id"] if images else None,
                "position": 1,
                "created_at": product_node.get("createdAt"),
                "updated_at": product_node.get("updatedAt"),
                "alt": product_node.get("featuredImage", {}).get("altText"),
                "width": product_node.get("featuredImage", {}).get("width"),
                "height": product_node.get("featuredImage", {}).get("height"),
                "src": product_node.get("featuredImage", {}).get("url")
            } if product_node.get("featuredImage") else None
        }
        
        return product
    
    def format_collection_for_json(self, collection_node):
        """Format collection data to match Shopify's JSON structure"""
        # Extract Shopify numeric ID from GraphQL ID
        gid = collection_node.get("id", "")
        numeric_id = gid.split("/")[-1] if "/" in gid else gid
        
        collection = {
            "id": numeric_id,
            "handle": collection_node.get("handle"),
            "title": collection_node.get("title"),
            "updated_at": collection_node.get("updatedAt"),
            "body_html": collection_node.get("descriptionHtml", ""),
            "published_at": collection_node.get("updatedAt"),
            "sort_order": collection_node.get("sortOrder", "best-selling"),
            "template_suffix": collection_node.get("templateSuffix"),
            "products_count": collection_node.get("productsCount", 0),
            "collection_type": "smart" if collection_node.get("sortOrder") else "custom",
            "published_scope": "web",
            "image": {
                "created_at": collection_node.get("updatedAt"),
                "alt": collection_node.get("image", {}).get("altText"),
                "width": collection_node.get("image", {}).get("width"),
                "height": collection_node.get("image", {}).get("height"),
                "src": collection_node.get("image", {}).get("url")
            } if collection_node.get("image") else None
        }
        
        return collection
    
    def create_collection_product_files(self, collections):
        """Create individual JSON files for each collection with its products"""
        print(f"Creating individual collection product files...", file=sys.stderr)
        
        # Create collections directory if it doesn't exist
        os.makedirs("collections", exist_ok=True)
        
        for collection in collections[:10]:  # Limit to first 10 for testing, remove limit in production
            handle = collection.get("handle")
            if not handle:
                continue
                
            print(f"Fetching products for collection: {handle}", file=sys.stderr)
            
            # Fetch products for this collection
            products = self.fetch_collection_products(handle)
            
            if products:
                # Create the collection products JSON structure
                collection_products = {
                    "collection": {
                        "id": collection.get("id"),
                        "handle": handle,
                        "title": collection.get("title"),
                        "products_count": len(products)
                    },
                    "products": products
                }
                
                # Save to collections/[handle]_products.json
                filename = f"collections/{handle}_products.json"
                self.save_json_file(filename, collection_products)
                print(f"Saved {len(products)} products for collection {handle}", file=sys.stderr)
    
    def execute_query(self, query, query_type):
        """Execute GraphQL query with detailed error reporting"""
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json={"query": query},
                timeout=60
            )
            
            if response.status_code != 200:
                print(f"HTTP Error for {query_type}: {response.status_code}", file=sys.stderr)
                return None
            
            data = response.json()
            
            if "errors" in data:
                print(f"GraphQL errors for {query_type}:", file=sys.stderr)
                for error in data["errors"]:
                    print(f"  - {error}", file=sys.stderr)
                return None
            
            return data
            
        except Exception as e:
            print(f"Error for {query_type}: {str(e)}", file=sys.stderr)
            return None
    
    def save_json_file(self, filename, data):
        """Save data to JSON file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Successfully saved {filename}", file=sys.stderr)
        except Exception as e:
            print(f"Error saving {filename}: {str(e)}", file=sys.stderr)
    
    def fetch_and_save_blogs(self):
        """Fetch and save blog data (keeping existing functionality)"""
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
                      contentHtml
                      createdAt
                      updatedAt
                      publishedAt
                      tags
                      image {
                        url
                        altText
                      }
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
            blogs = []
            for blog_edge in data.get("data", {}).get("blogs", {}).get("edges", []):
                blog = blog_edge["node"]
                blog_formatted = {
                    "id": blog["id"].split("/")[-1] if "/" in blog["id"] else blog["id"],
                    "title": blog["title"],
                    "handle": blog["handle"],
                    "created_at": blog["createdAt"],
                    "updated_at": blog["updatedAt"],
                    "articles": []
                }
                
                for article_edge in blog.get("articles", {}).get("edges", []):
                    article = article_edge["node"]
                    blog_formatted["articles"].append({
                        "id": article["id"].split("/")[-1] if "/" in article["id"] else article["id"],
                        "title": article["title"],
                        "handle": article["handle"],
                        "summary": article.get("summary"),
                        "content": article.get("contentHtml"),
                        "created_at": article["createdAt"],
                        "updated_at": article["updatedAt"],
                        "published_at": article.get("publishedAt"),
                        "tags": ", ".join(article.get("tags", [])) if article.get("tags") else "",
                        "image": article.get("image")
                    })
                
                blogs.append(blog_formatted)
            
            self.save_json_file("blogs.json", {"blogs": blogs})
            return True
        return False
    
    def cleanup_old_chunk_files(self):
        """Remove old chunk files that are no longer needed"""
        print("Cleaning up old chunk files...", file=sys.stderr)
        
        import glob
        
        # Patterns for old chunk files to remove
        old_file_patterns = [
            "products_chunk_*.json",
            "collections_with_products_chunk_*.json",
            "products_summary.json",
            "collections_with_products_summary.json",
            "blog_data.json",  # Old format
            "collections_with_products.json"  # Old large file
        ]
        
        removed_count = 0
        for pattern in old_file_patterns:
            files = glob.glob(pattern)
            for file in files:
                try:
                    os.remove(file)
                    print(f"Removed old file: {file}", file=sys.stderr)
                    removed_count += 1
                except Exception as e:
                    print(f"Error removing {file}: {e}", file=sys.stderr)
        
        if removed_count > 0:
            print(f"Cleaned up {removed_count} old files", file=sys.stderr)
    
    def create_data_index_files(self):
        """Create comprehensive index files with all data URLs"""
        print("Creating data index files...", file=sys.stderr)
        
        base_url = "https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/"
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # Get list of collection files
        collection_files = []
        if os.path.exists("collections"):
            for file in os.listdir("collections"):
                if file.endswith("_products.json"):
                    collection_files.append(file)
        collection_files.sort()
        
        # Create text index
        index_content = f"""SPACIRE SHOPIFY DATA INDEX
Generated: {timestamp}
Repository: sytrre/spacire-blog-data
Currency: GBP (British Pounds)

=== SHOPIFY-FORMAT JSON DATA ACCESS ===

MAIN DATA FILES:
----------------
• Products (All Products):
  {base_url}products.json
  Format: Shopify standard products.json with 250+ products per file
  Contains: Full product data including descriptions, variants, images, pricing in GBP

• Collections (All Collections):
  {base_url}collections.json
  Format: Shopify standard collections.json
  Contains: All collections with metadata, descriptions, product counts

• Blogs (All Blog Posts):
  {base_url}blogs.json
  Format: Blog posts with full HTML content
  Contains: All blog articles with summaries and full content

INDIVIDUAL COLLECTION FILES:
----------------------------
Each collection has its own product file with complete product data:
"""
        
        # Add collection files to text index
        for file in collection_files[:20]:  # Show first 20 in text file
            collection_handle = file.replace("_products.json", "")
            index_content += f"\n• {collection_handle}:\n  {base_url}collections/{file}"
        
        if len(collection_files) > 20:
            index_content += f"\n\n...and {len(collection_files) - 20} more collection files"
        
        index_content += f"""

=== USAGE INSTRUCTIONS ===

For Developers/API Integration:
1. Use the main files for bulk data access
2. Use individual collection files for specific collection products
3. All prices are in GBP (British Pounds)
4. Data updates every 30 minutes automatically

Data Structure:
- Matches Shopify's standard JSON format exactly
- Includes full HTML descriptions for products
- Complete variant data with inventory levels
- High-resolution images with dimensions
- SEO metadata included

=== UPDATE FREQUENCY ===
• Automatic sync: Every 30 minutes
• Manual trigger: Available via GitHub Actions
• Last updated: {timestamp}

=== QUICK ACCESS EXAMPLES ===

Get all products:
curl {base_url}products.json

Get specific collection products:
curl {base_url}collections/blackout-curtains_products.json

Get all blogs:
curl {base_url}blogs.json

=== TECHNICAL INFO ===
• Data source: Shopify GraphQL API
• Format: Standard Shopify JSON structure
• Pagination: 250 items per request
• Currency: GBP (British Pounds)
• Repository: Public for direct URL access
"""
        
        # Save text index
        with open("data_index.txt", "w", encoding="utf-8") as f:
            f.write(index_content)
        print("Created data_index.txt", file=sys.stderr)
        
        # Create JSON index
        json_index = {
            "generated": timestamp,
            "repository": "sytrre/spacire-blog-data",
            "base_url": base_url,
            "currency": "GBP",
            "update_frequency": "30 minutes",
            "data_format": "Shopify standard JSON",
            "files": {
                "products": {
                    "url": f"{base_url}products.json",
                    "description": "All products with full data",
                    "format": "Shopify products.json",
                    "contains": "Complete product catalog with descriptions, variants, images, pricing in GBP"
                },
                "collections": {
                    "url": f"{base_url}collections.json",
                    "description": "All collections",
                    "format": "Shopify collections.json",
                    "contains": "All collections with metadata and product counts"
                },
                "blogs": {
                    "url": f"{base_url}blogs.json",
                    "description": "All blog posts",
                    "format": "Blog JSON with HTML content",
                    "contains": "All blog articles with full content"
                },
                "collection_products": {
                    "base_url": f"{base_url}collections/",
                    "description": "Individual collection product files",
                    "format": "Shopify collection products.json",
                    "files": {}
                }
            },
            "total_files": {
                "main_files": 3,
                "collection_files": len(collection_files)
            }
        }
        
        # Add collection files to JSON index
        for file in collection_files:
            handle = file.replace("_products.json", "")
            json_index["files"]["collection_products"]["files"][handle] = {
                "url": f"{base_url}collections/{file}",
                "handle": handle
            }
        
        # Save JSON index
        with open("data_index.json", "w", encoding="utf-8") as f:
            json.dump(json_index, f, indent=2, ensure_ascii=False)
        print("Created data_index.json", file=sys.stderr)

def main():
    # Get credentials from environment variables
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    sync_type = os.getenv("SYNC_TYPE", "all")
    
    if not shop_domain or not access_token:
        print("Error: Missing required environment variables", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting data fetch for {shop_domain} (sync_type: {sync_type})", file=sys.stderr)
    
    # Initialize fetcher
    fetcher = ShopifyDataFetcher(shop_domain, access_token)
    
    # Test API connection
    if not fetcher.test_api_connection():
        print("Failed to connect to API", file=sys.stderr)
        sys.exit(1)
    
    # Clean up old chunk files first
    if sync_type == "all":
        print("\n=== Cleaning Up Old Files ===", file=sys.stderr)
        fetcher.cleanup_old_chunk_files()
    
    # Based on sync type, fetch different data
    if sync_type in ["all", "products"]:
        print("\n=== Fetching Products ===", file=sys.stderr)
        fetcher.fetch_all_products()
    
    if sync_type in ["all", "collections"]:
        print("\n=== Fetching Collections ===", file=sys.stderr)
        collections = fetcher.fetch_all_collections()
        
        # Create individual collection files (optional, can be disabled for performance)
        if sync_type == "all" and collections:
            print("\n=== Creating Collection Product Files ===", file=sys.stderr)
            fetcher.create_collection_product_files(collections)
    
    if sync_type in ["all", "blogs"]:
        print("\n=== Fetching Blogs ===", file=sys.stderr)
        fetcher.fetch_and_save_blogs()
    
    # Create index files with all URLs
    if sync_type in ["all"]:
        print("\n=== Creating Data Index Files ===", file=sys.stderr)
        fetcher.create_data_index_files()
    
    print("\n✅ Data fetch completed successfully!", file=sys.stderr)

if __name__ == "__main__":
    main()
