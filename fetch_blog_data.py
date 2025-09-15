#!/usr/bin/env python3
"""
Spacire Shopify Data Fetcher - Debug Version
Tests each query separately with detailed error reporting
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
        """Fetch collections with minimal fields first"""
        print("Fetching collections (simple)...", file=sys.stderr)
        
        query = """
        {
          collections(first: 50) {
            edges {
              node {
                id
                title
                handle
                description
              }
            }
          }
        }
        """
        
        data = self.execute_query(query, "collections_simple")
        if data:
            result = self.process_simple_collections(data)
            self.save_json_file("collections.json", result)
            return True
        return False
    
    def fetch_and_save_products_simple(self):
        """Fetch products with minimal fields first"""
        print("Fetching products (simple)...", file=sys.stderr)
        
        query = """
        {
          products(first: 50) {
            edges {
              node {
                id
                title
                handle
                createdAt
                updatedAt
                productType
                vendor
                tags
              }
            }
          }
        }
        """
        
        data = self.execute_query(query, "products_simple")
        if data:
            result = self.process_simple_products(data)
            self.save_json_file("products.json", result)
            return True
        return False
    
    def fetch_and_save_collections_with_products(self):
        """Fetch collections with products"""
        print("Fetching collections with products...", file=sys.stderr)
        
        query = """
        {
          collections(first: 20) {
            edges {
              node {
                id
                title
                handle
                description
                products(first: 50) {
                  edges {
                    node {
                      id
                      title
                      handle
                      productType
                      vendor
                      createdAt
                      updatedAt
                      tags
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        data = self.execute_query(query, "collections_with_products")
        if data:
            result = self.process_collections_with_products(data)
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
    
    def process_simple_collections(self, raw_data):
        """Process simple collections data"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "collections",
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
                "description": collection.get("description")
            }
            
            processed_data["collections"].append(collection_info)
        
        return processed_data
    
    def process_simple_products(self, raw_data):
        """Process simple products data"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "products",
            "total_products": 0,
            "products": []
        }
        
        products_data = raw_data.get("data", {}).get("products", {}).get("edges", [])
        processed_data["total_products"] = len(products_data)
        
        for product_edge in products_data:
            product = product_edge["node"]
            
            product_info = {
                "product_id": product["id"],
                "title": product["title"],
                "handle": product["handle"],
                "product_type": product.get("productType"),
                "vendor": product.get("vendor"),
                "created_at": product["createdAt"],
                "updated_at": product["updatedAt"],
                "tags": product.get("tags", [])
            }
            
            processed_data["products"].append(product_info)
        
        return processed_data
    
    def process_collections_with_products(self, raw_data):
        """Process collections with products"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        processed_data = {
            "fetch_timestamp": timestamp,
            "shop_domain": self.shop_domain,
            "data_type": "collections_with_products",
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
                "products": []
            }
            
            products_data = collection.get("products", {}).get("edges", [])
            
            for product_edge in products_data:
                product = product_edge["node"]
                
                product_info = {
                    "product_id": product["id"],
                    "title": product["title"],
                    "handle": product["handle"],
                    "product_type": product.get("productType"),
                    "vendor": product.get("vendor"),
                    "created_at": product["createdAt"],
                    "updated_at": product["updatedAt"],
                    "tags": product.get("tags", [])
                }
                
                collection_info["products"].append(product_info)
            
            collection_info["products_count"] = len(collection_info["products"])
            processed_data["collections"].append(collection_info)
        
        return processed_data
    
    def save_json_file(self, filename, data):
        """Save data to JSON file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Successfully saved {filename} with {len(json.dumps(data))} characters", file=sys.stderr)
        except Exception as e:
            print(f"Error saving {filename}: {str(e)}", file=sys.stderr)

def main():
    # Get credentials from environment variables
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("Error: Missing required environment variables", file=sys.stderr)
        print("Please set SHOPIFY_SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting data fetch for {shop_domain}", file=sys.stderr)
    
    # Initialize fetcher
    fetcher = ShopifyDataFetcher(shop_domain, access_token)
    
    # Test API connection first
    if not fetcher.test_api_connection():
        print("Failed to connect to API", file=sys.stderr)
        sys.exit(1)
    
    # Fetch each data type separately
    success_count = 0
    
    if fetcher.fetch_and_save_blogs():
        success_count += 1
    
    if fetcher.fetch_and_save_collections_simple():
        success_count += 1
    
    if fetcher.fetch_and_save_products_simple():
        success_count += 1
    
    if fetcher.fetch_and_save_collections_with_products():
        success_count += 1
    
    print(f"Successfully created {success_count} out of 4 data files", file=sys.stderr)
    
    if success_count == 0:
        print("No data files were created - check API permissions", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
