# Spacire Shopify Data API

**Auto-updates every 30 minutes | All prices in GBP**

## Quick Access - Shopify Standard JSON

| Data Type | URL |
|-----------|-----|
| All Products | [products.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json) |
| All Collections | [collections.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections.json) |
| All Blogs | [blogs.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/blogs.json) |
| Data Index | [data_index.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/data_index.json) |

## Pagination

All files are paginated at 250 items per page. Files with more than 250 items have additional pages:
- `products_page2.json`, `products_page3.json`, etc.
- Check `pagination` field in each file for navigation

## Collection Products

Each collection has its own products file(s) in the `collections/` directory:
- [Sleep Masks](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/sleep-masks_products.json)
- [Weighted Blankets](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/weighted-blankets_products.json)
- [View all collections →](./collections)

## API Usage

```javascript
// Fetch first page of products
fetch('https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json')
  .then(res => res.json())
  .then(data => {
    console.log(data.products); // Array of up to 250 products
    console.log(data.pagination); // Pagination info
    
    // Check for more pages
    if (data.pagination.has_next_page) {
      // Fetch next page: products_page2.json
    }
  });
```

Last updated: $(date)
