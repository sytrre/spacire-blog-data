#!/usr/bin/env python3
"""
Spacire Shopify Blog Data Fetcher
Fetches blog metadata from Shopify using GraphQL API
"""

import os
import json
import requests
from datetime import datetime
import sys

class ShopifyBlogFetcher:
    def __init__(self, shop_domain, access_token):
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_url = f"https://{shop_domain}/admin/api/2023-10/graphql.json"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }
    
    def fetch_blogs_and_articles(self):
        """Fetch all blogs and their articles (both published and unpublished)"""
        
        # GraphQL query to get all blogs and articles
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
            
            return self.process_blog_data(data)
            
        except Exception as e:
            print(f"Error fetching data: {str(e)}", file=sys.stderr)
            return None
    
    def process_blog_data(self, raw_data):
        """Process and structure the blog data"""
        
        processed_data = {
            "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
            "shop_domain": self.shop_domain,
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
                
                # Determine status based on publishedAt field
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

def main():
    # Get credentials from environment variables
    shop_domain = os.getenv("SHOPIFY_SHOP_DOMAIN")
    access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    
    if not shop_domain or not access_token:
        print("Error: Missing required environment variables", file=sys.stderr)
        print("Please set SHOPIFY_SHOP_DOMAIN and SHOPIFY_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    # Initialize fetcher
    fetcher = ShopifyBlogFetcher(shop_domain, access_token)
    
    # Fetch blog data
    blog_data = fetcher.fetch_blogs_and_articles()
    
    if blog_data is None:
        sys.exit(1)
    
    # Output JSON to stdout (for GitHub Pages to serve)
    print(json.dumps(blog_data, indent=2, ensure_ascii=False))
    
    # Also save to file for GitHub Pages
    output_file = "blog_data.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(blog_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nData successfully saved to {output_file}", file=sys.stderr)
        print(f"Total blogs: {blog_data['total_blogs']}", file=sys.stderr)
        print(f"Total articles: {blog_data['total_articles']}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error saving file: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
