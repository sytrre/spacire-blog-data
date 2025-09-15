#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher
Fetches blog, collection, and product metadata from Shopify using GraphQL API
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
    
    def fetch_all_data(self):
        """Fetch all data: blogs, collections, and products"""
        
        print("Fetching blog data...", file=sys.stderr)
        blogs_data = self.fetch_blogs_and_articles()
        
        print("Fetching collections data...", file=sys.stderr)
        collections_data = self.fetch_collections()
        
        print("Fetching active products data...", file=sys.stderr)
        products_data = self.fetch_active_products()
        
        # Process all data
        processed_data = {
            "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
            "shop_domain": self.shop_domain,
            "data": {}
        }
        
        # Process blogs
        if blogs_data:
            processed_data["data"]["blogs"] = self.process_blog_data(blogs_data)
        
        # Process collections
        if collections_data:
            processed_data["data"]["collections"] = self.process_collections_data(collections_data)
        
        # Process products
        if products_data:
            processed_data["data"]["products"] = self.process_products_data(products_data)
        
        return processed_data
    
    def fetch_blogs_and_articles(self):
        """Fetch all blogs and their articles"""
        
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
        
        return self.execute_query(query, "blogs")
    
    def fetch_collections(self):
        """Fetch all collections and their products"""
        
        query = """
        {
          collections(first: 250) {
            edges {
              node {
                id
                title
                handle
                description
                descriptionHtml
                createdAt
                updatedAt
                productsCount
                products(first: 250) {
                  edges {
                    node {
                      id
                      title
                      handle
                      description
                      productType
                      vendor
                      status
                      createdAt
                      updatedAt
                      publishedAt
                      tags
                      featuredImage {
                        url
                        altText
                      }
                      priceRangeV2 {
                        minVariantPrice {
                          amount
                          currencyCode
                        }
                        maxVariantPrice {
                          amount
                          currencyCode
                        }
                      }
                      variants(first: 100) {
                        edges {
                          node {
                            id
                            title
                            sku
                            availableForSale
                            price
                            compareAtPrice
                            createdAt
                            updatedAt
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        return self.execute_query(query, "collections")
    
    def fetch_active_products(self):
        """Fetch all active products"""
        
        query = """
        {
          products(first: 250, query: "status:active") {
            edges {
              node {
                id
                title
                handle
                description
                descriptionHtml
                productType
                vendor
                status
                createdAt
                updatedAt
                publishedAt
                tags
                featuredImage {
                  url
                  altText
                }
                images(first: 10) {
                  edges {
                    node {
                      url
                      altText
                    }
                  }
                }
                priceRangeV2 {
                  minVariantPrice {
                    amount
                    currencyCode
                  }
                  maxVariantPrice {
                    amount
                    currencyCode
                  }
                }
                compareAtPriceRange {
                  minVariantPrice {
                    amount
                    currencyCode
                  }
                  maxVariantPrice {
                    amount
                    currencyCode
                  }
                }
                variants(first: 100) {
                  edges {
                    node {
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
                      selectedOptions {
                        name
                        value
                      }
                    }
                  }
                }
                options {
                  name
                  values
                }
              }
            }
          }
        }
        """
        
        return self.execute_query(query, "products")
    
    def execute_query(self, query, query_type):
        """Execute GraphQL query and handle errors"""
        
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json={"query": query}
            )
            
            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")
            
            data = response.json()
            
            if "errors" in data:
                raise Exception(f"GraphQL errors: {data['errors']}")
            
            return data
            
        except Exception as e:
            print(f"Error fetching {query_type}: {str(e)}", file=sys.stderr)
            return None
    
    def process_blog_data(self, raw_data):
        """Process blog data"""
        
        if not raw_data:
            return {"blogs": [], "total_blogs": 0, "total_articles": 0}
        
        processed_data = {
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
                status = "PUBLISHED" if is_published else "DRAFT"
                
                article_info = {
                    "article_id": article["id"],
                    "title": article["title"],
                    "handle": article["handle"],
                    "summary": article.get("summary"),
                    "status": status,
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
    
    def process_collections_data(self, raw_data):
        """Process collections data"""
        
        if not raw_data:
            return {"collections": [], "total_collections": 0}
        
        processed_data = {
            "total_collections": 0,
            "collections": []
        }
        
        collections_data = raw_data.get("data", {}).get("collections", {}).get("edges", [])
        processed_data["total_collections"] = len(collections_data)
        
        for collection_edge in collections_data:
            collection = collection_edge["node"]
            
            collection_info = {
                "collection_id": collection["id"],
                "title": collection["title"],
                "handle": collection["handle"],
                "description": collection.get("description"),
                "description_html": collection.get("descriptionHtml"),
                "created_at": collection["createdAt"],
                "updated_at": collection["updatedAt"],
                "products_count": collection.get("productsCount", 0),
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
                
                # Process price range
                price_range = product.get("priceRangeV2", {})
                min_price = price_range.get("minVariantPrice", {})
                max_price = price_range.get("maxVariantPrice", {})
                
                product_info = {
                    "product_id": product["id"],
                    "title": product["title"],
                    "handle": product["handle"],
                    "description": product.get("description"),
                    "product_type": product.get("productType"),
                    "vendor": product.get("vendor"),
                    "status": product["status"],
                    "created_at": product["createdAt"],
                    "updated_at": product["updatedAt"],
                    "published_at": product.get("publishedAt"),
                    "tags": product.get("tags", []),
                    "featured_image": {
                        "url": product.get("featuredImage", {}).get("url"),
                        "alt_text": product.get("featuredImage", {}).get("altText")
                    } if product.get("featuredImage") else None,
                    "price_range": {
                        "min_price": {
                            "amount": min_price.get("amount"),
                            "currency_code": min_price.get("currencyCode")
                        },
                        "max_price": {
                            "amount": max_price.get("amount"),
                            "currency_code": max_price.get("currencyCode")
                        }
                    },
                    "variants": variants,
                    "variants_count": len(variants)
                }
                
                collection_info["products"].append(product_info)
            
            processed_data["collections"].append(collection_info)
        
        return processed_data
    
    def process_products_data(self, raw_data):
        """Process products data"""
        
        if not raw_data:
            return {"products": [], "total_products": 0}
        
        processed_data = {
            "total_products": 0,
            "products": []
        }
        
        products_data = raw_data.get("data", {}).get("products", {}).get("edges", [])
        processed_data["total_products"] = len(products_data)
        
        for product_edge in products_data:
            product = product_edge["node"]
            
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
            
            # Process price ranges
            price_range = product.get("priceRangeV2", {})
            min_price = price_range.get("minVariantPrice", {})
            max_price = price_range.get("maxVariantPrice", {})
            
            compare_at_price_range = product.get("compareAtPriceRange", {})
            compare_min_price = compare_at_price_range.get("minVariantPrice", {}) if compare_at_price_range else {}
            compare_max_price = compare_at_price_range.get("maxVariantPrice", {}) if compare_at_price_range else {}
            
            product_info = {
                "product_id": product["id"],
                "title": product["title"],
                "handle": product["handle"],
                "description": product.get("description"),
                "description_html": product.get("descriptionHtml"),
                "product_type": product.get("productType"),
                "vendor": product.get("vendor"),
                "status": product["status"],
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
                "price_range": {
                    "min_price": {
                        "amount": min_price.get("amount"),
                        "currency_code": min_price.get("currencyCode")
                    },
                    "max_price": {
                        "amount": max_price.get("amount"),
                        "currency_code": max_price.get("currencyCode")
                    }
                },
                "compare_at_price_range": {
                    "min_price": {
                        "amount": compare_min_price.get("amount"),
                        "currency_code": compare_min_price.get("currencyCode")
                    },
                    "max_price": {
                        "amount": compare_max_price.get("amount"),
                        "currency_code": compare_max_price.get("currencyCode")
                    }
                } if compare_at_price_range else None,
                "variants": variants,
                "variants_count": len(variants),
                "options": options
            }
            
            processed_data["products"].append(product_info)
        
        return processed_data

def main():
    # Get credentials from environment variables
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("Error: Missing required environment variables", file=sys.stderr)
        print("Please set SHOPIFY_SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    # Initialize fetcher
    fetcher = ShopifyDataFetcher(shop_domain, access_token)
    
    # Fetch all data
    all_data = fetcher.fetch_all_data()
    
    if all_data is None:
        sys.exit(1)
    
    # Output JSON to stdout (for GitHub to serve)
    print(json.dumps(all_data, indent=2, ensure_ascii=False))
    
    # Also save to file
    output_file = "shopify_data.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        
        # Print summary
        blog_data = all_data.get("data", {}).get("blogs", {})
        collections_data = all_data.get("data", {}).get("collections", {})
        products_data = all_data.get("data", {}).get("products", {})
        
        print(f"\nData successfully saved to {output_file}", file=sys.stderr)
        print(f"Total blogs: {blog_data.get('total_blogs', 0)}", file=sys.stderr)
        print(f"Total articles: {blog_data.get('total_articles', 0)}", file=sys.stderr)
        print(f"Total collections: {collections_data.get('total_collections', 0)}", file=sys.stderr)
        print(f"Total active products: {products_data.get('total_products', 0)}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error saving file: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
