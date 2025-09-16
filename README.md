# Spacire Shopify Data API

**Auto-updates every 30 minutes | All prices in GBP**

## Quick Access - Shopify Standard JSON

| Data Type | URL |
|-----------|-----|
| All Products | [products.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json) |
| All Collections | [collections.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections.json) |
| All Blogs | [blogs.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/blogs.json) |
| Data Index | [data_index.json](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/data_index.json) |

## Collection-Specific Files

Each collection has its own products file in the `collections/` directory:
- [Blackout Curtains](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/blackout-curtains_products.json)
- [Sleep Masks](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/sleep-masks_products.json)
- [Weighted Blankets](https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/weighted-blankets_products.json)
- [View all collections →](./collections)

## API Usage

```javascript
// Fetch all products
fetch('https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/products.json')
  .then(res => res.json())
  .then(data => {
    console.log(data.products); // Array of products
  });

// Fetch specific collection
fetch('https://raw.githubusercontent.com/sytrre/spacire-blog-data/refs/heads/main/collections/sleep-masks_products.json')
  .then(res => res.json())
  .then(data => {
    console.log(data.products); // Products in this collection
  });
```

Last updated: $(date)
